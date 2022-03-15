/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2021 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"

#include "vbdev_congctrl.h"
#include "spdk/rpc.h"
#include "spdk/env.h"
#include "spdk/endian.h"
#include "spdk/string.h"
#include "spdk/thread.h"
#include "spdk/util.h"
#include "spdk/likely.h"

#include "spdk/bdev_module.h"
#include "spdk/log.h"

#define SPDK_NVME_CC_CWND_MAX_IOCOUNT 1024
#define SPDK_NVME_CC_CWND_SIZE_MIN (4096UL)
#define SPDK_NVME_CC_RATE_QUANTUM (131072UL)
#define SPDK_NVME_CC_LATHRESH_MAX_USEC 1500
#define SPDK_NVME_CC_LATHRESH_MIN_USEC 250
#define SPDK_NVME_CC_NUM_FLOW_MAX 0xFFFF

#define SPDK_NVME_CC_EWMA_PARAM 	  4	// Weight = 1/(2^SPDK_NVME_CC_EWMA_PARAM)
#define SPDK_NVME_CC_LATHRES_EWMA_PARAM 4	// Weight = 1/(2^SPDK_NVME_CC_LATHRES_EWMA_PARAM)

#define SPDK_NVME_CC_WEIGHT_PARAM (10)
#define SPDK_NVME_CC_WEIGHT_ONE (1<<SPDK_NVME_CC_WEIGHT_PARAM)

#define SPDK_NVME_CC_CONG_PARAM (10)

enum spdk_nvme_cc_rate_state {
	SPDK_NVME_CC_RATE_SUBMITTABLE,
	SPDK_NVME_CC_RATE_SUBMITTABLE_READONLY,
	SPDK_NVME_CC_RATE_DEFERRED,
	SPDK_NVME_CC_RATE_OVERLOADED,
	SPDK_NVME_CC_RATE_CONGESTION,
	SPDK_NVME_CC_RATE_SLOWSTART,
	SPDK_NVME_CC_RATE_DRAINING,
	SPDK_NVME_CC_RATE_OK
};

enum spdk_nvme_cc_sched_type {
	SPDK_NVME_CC_IOSCHED_DRR,
	SPDK_NVME_CC_IOSCHED_WDRR,
	SPDK_NVME_CC_IOSCHED_NOOP
};

#define SPDK_NVME_CC_EWMA(ewma, raw, param) \
	((raw >> param) + (ewma) - (ewma >> param))

static int vbdev_congctrl_init(void);
static int vbdev_congctrl_get_ctx_size(void);
static void vbdev_congctrl_examine(struct spdk_bdev *bdev);
static void vbdev_congctrl_finish(void);
static int vbdev_congctrl_config_json(struct spdk_json_write_ctx *w);

static struct spdk_bdev_module congctrl_if = {
	.name = "congctrl",
	.module_init = vbdev_congctrl_init,
	.get_ctx_size = vbdev_congctrl_get_ctx_size,
	.examine_config = vbdev_congctrl_examine,
	.module_fini = vbdev_congctrl_finish,
	.config_json = vbdev_congctrl_config_json
};

SPDK_BDEV_MODULE_REGISTER(congctrl, &congctrl_if)

/* Data structures for congestion control */
struct congctrl_cong {
	uint64_t thresh_ticks;
	uint64_t thresh_upper;
	uint64_t thresh_lower;
	uint64_t thresh_residue;
	uint64_t ewma_ticks;
	uint64_t total_lat_ticks;
	uint64_t total_io;
	uint64_t total_bytes;

	uint32_t congestion_count;
	uint64_t last_stat_cong;
	uint32_t overloaded_count;

	int64_t rate;
	uint64_t last_update_tsc;
	uint64_t max_bucket_size;
	uint64_t tokens;
};

/* Associative list to be used in examine */
struct bdev_association {
	char			*vbdev_name;
	char			*bdev_name;
	uint64_t		upper_read_latency;
	uint64_t		lower_read_latency;
	uint64_t		upper_write_latency;
	uint64_t		lower_write_latency;
	TAILQ_ENTRY(bdev_association)	link;
};
static TAILQ_HEAD(, bdev_association) g_bdev_associations = TAILQ_HEAD_INITIALIZER(
			g_bdev_associations);

/* List of virtual bdevs and associated info for each. */
struct vbdev_congctrl {
	struct spdk_bdev		*base_bdev; /* the thing we're attaching to */
	struct spdk_bdev_desc		*base_desc; /* its descriptor we get from open */
	struct spdk_bdev		congctrl_bdev;    /* the congctrl virtual bdev */
	uint64_t			upper_read_latency; /* the upper read latency */
	uint64_t			lower_read_latency; /* the lower read latency */
	uint64_t			upper_write_latency; /* the upper write latency */
	uint64_t			lower_write_latency; /* the lower write latency */
	TAILQ_ENTRY(vbdev_congctrl)	link;
	struct spdk_thread		*thread;    /* thread where base device is opened */
};
static TAILQ_HEAD(, vbdev_congctrl) g_congctrl_nodes = TAILQ_HEAD_INITIALIZER(g_congctrl_nodes);

typedef int (*congctrl_io_wait_cb)(void *cb_arg);

struct congctrl_io_wait_entry {
	congctrl_io_wait_cb			cb_fn;
	void					*cb_arg;
	TAILQ_ENTRY(congctrl_io_wait_entry)	link;
};

struct congctrl_bdev_io {
	int status;

	uint64_t submit_tick;
	uint64_t completion_tick;
	
	struct congctrl_cong *cong;

	enum congctrl_lat_type type;

	struct spdk_io_channel *ch;

	struct spdk_bdev_io_wait_entry bdev_io_wait;

	struct congctrl_io_wait_entry congctrl_io_wait;

	STAILQ_ENTRY(congctrl_bdev_io) link;
};

struct congctrl_io_channel {
	struct spdk_io_channel	*base_ch; /* IO channel of base device */

	struct congctrl_cong	rd_cong;
	struct congctrl_cong	wr_cong;

	TAILQ_HEAD(, congctrl_io_wait_entry)	read_io_wait_queue;
	TAILQ_HEAD(, congctrl_io_wait_entry)	write_io_wait_queue;
	struct spdk_poller *top_poller;
	struct spdk_poller *io_poller;
	unsigned int rand_seed;
};

static void
vbdev_congctrl_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io);


/* Callback for unregistering the IO device. */
static void
_device_unregister_cb(void *io_device)
{
	struct vbdev_congctrl *congctrl_node  = io_device;

	/* Done with this congctrl_node. */
	free(congctrl_node->congctrl_bdev.name);
	free(congctrl_node);
}

static void
_vbdev_congctrl_destruct(void *ctx)
{
	struct spdk_bdev_desc *desc = ctx;

	spdk_bdev_close(desc);
}

static int
vbdev_congctrl_destruct(void *ctx)
{
	struct vbdev_congctrl *congctrl_node = (struct vbdev_congctrl *)ctx;

	/* It is important to follow this exact sequence of steps for destroying
	 * a vbdev...
	 */

	TAILQ_REMOVE(&g_congctrl_nodes, congctrl_node, link);

	/* Unclaim the underlying bdev. */
	spdk_bdev_module_release_bdev(congctrl_node->base_bdev);

	/* Close the underlying bdev on its same opened thread. */
	if (congctrl_node->thread && congctrl_node->thread != spdk_get_thread()) {
		spdk_thread_send_msg(congctrl_node->thread, _vbdev_congctrl_destruct, congctrl_node->base_desc);
	} else {
		spdk_bdev_close(congctrl_node->base_desc);
	}

	/* Unregister the io_device. */
	spdk_io_device_unregister(congctrl_node, _device_unregister_cb);

	return 0;
}

static inline void
_congctrl_cong_init(struct congctrl_cong *cong, uint64_t upper_latency, uint64_t lower_latency)
{
	cong->ewma_ticks = 0;
	cong->thresh_upper = upper_latency;
	cong->thresh_lower = lower_latency;
	cong->thresh_ticks = cong->thresh_upper;
	cong->thresh_residue = 0;
	cong->last_update_tsc = spdk_get_ticks();
	cong->rate = (1000UL) * 1024 * 1024;
	//cong->rate = 0;
	cong->tokens = 128*1024;	// TODO: to be MDTS of the device
	cong->max_bucket_size = 16*128*1024;
}

static inline int
_congctrl_cong_latency_update(struct congctrl_cong *cong, uint64_t iolen, uint64_t latency_ticks)
{
	cong->total_lat_ticks += latency_ticks;
	cong->total_bytes += iolen;
	cong->total_io++;

	//cong->ewma_ticks = SPDK_NVME_CC_EWMA(cong->ewma_ticks,
	//									 latency_ticks, SPDK_NVME_CC_EWMA_PARAM);
	cong->ewma_ticks = latency_ticks;
	//cong->thresh_ticks = cong->thresh_upper;

	if (cong->ewma_ticks > cong->thresh_upper) {
		cong->thresh_ticks = (cong->thresh_ticks
						+ cong->thresh_upper) / 2;
		cong->thresh_residue = 0;
		cong->congestion_count++;
		cong->overloaded_count++;
		return SPDK_NVME_CC_RATE_OVERLOADED;
	} else if (cong->ewma_ticks > cong->thresh_ticks) {
		cong->thresh_ticks = (cong->thresh_ticks
						+ cong->thresh_upper) / 2;
		cong->thresh_residue = 0;
		cong->congestion_count++;
		return SPDK_NVME_CC_RATE_CONGESTION;
	} else {
		cong->thresh_residue += (cong->thresh_ticks - cong->ewma_ticks);
		cong->thresh_ticks -= cong->thresh_residue >> SPDK_NVME_CC_LATHRES_EWMA_PARAM;
		cong->thresh_residue = cong->thresh_residue & ((1 << SPDK_NVME_CC_LATHRES_EWMA_PARAM) - 1);
		return SPDK_NVME_CC_RATE_OK;
	}
}

static inline void
_congctrl_cong_token_refill(struct congctrl_cong *cong, uint64_t now)
{
	cong->tokens += cong->rate * (now - cong->last_update_tsc) / spdk_get_ticks_hz();
	cong->tokens = spdk_min(cong->tokens, cong->max_bucket_size);
	cong->last_update_tsc = now;

	return;
}

static inline void
_congctrl_cong_token_done(struct congctrl_cong *cong, uint64_t now, uint64_t iolen)
{
	return;
}

static inline void
_congctrl_cong_token_update(struct congctrl_cong *cong, int cong_res, uint64_t iolen) 
{
	if (cong_res == SPDK_NVME_CC_RATE_OVERLOADED) {
		cong->tokens = 0;
		//cong->rate -= iolen;
		cong->rate -= 16*iolen;
		//cong->rate = cong->rate*95/100;
	} else if (cong_res == SPDK_NVME_CC_RATE_CONGESTION) {
		cong->rate -= iolen;
	} else if (cong_res == SPDK_NVME_CC_RATE_SLOWSTART) {
		//cong->rate += 8*iolen;
		cong->rate += iolen;
	} else {
		cong->rate += iolen;
	}

	cong->rate = spdk_max(cong->rate, 1024*1024);
	//cong->rate = 50*1024*1024;
}

static inline uint64_t
_congctrl_cong_token_get(struct congctrl_io_channel *congctrl_ch, enum congctrl_lat_type type, uint64_t iolen) 
{
	struct congctrl_cong *cong;

	switch(type) {
	case LATENCY_READ:
		cong = &congctrl_ch->rd_cong;
		break;
	case LATENCY_WRITE:
		cong = &congctrl_ch->wr_cong;
		break;
	default:
		return -EINVAL;
	}

	//SPDK_NOTICELOG("_congctrl_cong_token_get: tokens:%lu iolen:%lu\n", cong->tokens, iolen);
	if (cong->tokens < iolen) {
		return -EAGAIN;
	}
	cong->tokens -= iolen;

	return iolen;
}

static inline void
_congctrl_cong_cwnd_refill(struct congctrl_cong *cong, uint64_t now)
{
	return;
}

static inline void
_congctrl_cong_cwnd_done(struct congctrl_cong *cong, uint64_t now, uint64_t iolen)
{
	cong->rate -= iolen;
	return;
}

static inline void
_congctrl_cong_cwnd_update(struct congctrl_cong *cong, int cong_res, uint64_t iolen) 
{
	switch (cong_res) {
	case SPDK_NVME_CC_RATE_OVERLOADED:
	case SPDK_NVME_CC_RATE_CONGESTION:
		//cong->tokens = cong->tokens >> 1;
		cong->tokens -= iolen;
		break;
	case SPDK_NVME_CC_RATE_SLOWSTART:
		//cong->tokens += iolen;
		//break;
	case SPDK_NVME_CC_RATE_OK:
		cong->tokens += iolen * (128*1024) / cong->tokens;
		break;
	default:
		break;
	}

	cong->tokens = spdk_min(cong->tokens, cong->max_bucket_size);
}

static inline uint64_t
_congctrl_cong_cwnd_get(struct congctrl_io_channel *congctrl_ch, enum congctrl_lat_type type, uint64_t iolen) 
{
	struct congctrl_cong *cong;

	switch(type) {
	case LATENCY_READ:
		cong = &congctrl_ch->rd_cong;
		break;
	case LATENCY_WRITE:
		cong = &congctrl_ch->wr_cong;
		break;
	default:
		return -EINVAL;
	}

	if (spdk_unlikely(cong->tokens < iolen && cong->rate == 0)) {
			cong->tokens = iolen;
	} else if (cong->tokens < cong->rate + iolen) {
		//printf("EAGAIN: %ld/%lu\n", cong->rate, cong->tokens);
		return -EAGAIN;
	}
	cong->rate += iolen;
	//printf("SUCCESS: %ld/%lu\n", cong->rate, cong->tokens);
	return iolen;
}

static int
_resubmit_io_tailq(void *arg)
{
	TAILQ_HEAD(, congctrl_io_wait_entry) *head = arg;
	struct congctrl_io_wait_entry *entry, *tmp;
	int submissions = 0;

	TAILQ_FOREACH_SAFE(entry, head, link, tmp) {
		if (entry->cb_fn(entry->cb_arg) < 0) {
			break;
		} else {
			TAILQ_REMOVE(head, entry, link);
			submissions++;
		}
	}
	return submissions;
}

static int
_congctrl_cong_top(void *arg)
{
	struct congctrl_io_channel *congctrl_ch = arg;
	uint64_t win_lat=0, win_bw=0, win_cap=0;

	if (congctrl_ch->rd_cong.total_io) {
		win_lat = SPDK_SEC_TO_USEC * congctrl_ch->rd_cong.total_lat_ticks
					/ congctrl_ch->rd_cong.total_io / spdk_get_ticks_hz(); 
		win_bw = congctrl_ch->rd_cong.total_bytes * SPDK_SEC_TO_USEC / 1000000;
		win_cap = congctrl_ch->rd_cong.total_bytes * SPDK_SEC_TO_USEC / congctrl_ch->rd_cong.total_lat_ticks;
	}
	/*
	printf("ch:%p r_rate:%ld r_ewma:%llu r_thresh:%llu r_token:%lu win_bw:%lu\n",
			congctrl_ch,
			congctrl_ch->rd_cong.rate>>20,
			congctrl_ch->rd_cong.ewma_ticks*SPDK_SEC_TO_USEC/spdk_get_ticks_hz(),
			congctrl_ch->rd_cong.thresh_ticks*SPDK_SEC_TO_USEC/spdk_get_ticks_hz(),
			congctrl_ch->rd_cong.tokens>>10,
			win_bw >> 20
			);
	*/
	
	printf("ch:%p r_rate:%ld r_ewma:%llu r_thresh:%llu r_token:%lu w_rate:%ld w_ewma:%llu w_thresh:%llu w_token:%lu\n",
			congctrl_ch,
			congctrl_ch->rd_cong.rate>>20,
			congctrl_ch->rd_cong.ewma_ticks*SPDK_SEC_TO_USEC/spdk_get_ticks_hz(),
			congctrl_ch->rd_cong.thresh_ticks*SPDK_SEC_TO_USEC/spdk_get_ticks_hz(),
			congctrl_ch->rd_cong.tokens>>10,
			congctrl_ch->wr_cong.rate>>20,
			congctrl_ch->wr_cong.ewma_ticks*SPDK_SEC_TO_USEC/spdk_get_ticks_hz(),
			congctrl_ch->wr_cong.thresh_ticks*SPDK_SEC_TO_USEC/spdk_get_ticks_hz(),
			congctrl_ch->wr_cong.tokens>>10);
	
	congctrl_ch->rd_cong.total_lat_ticks = 0;
	congctrl_ch->rd_cong.total_io = 0;
	congctrl_ch->rd_cong.total_bytes = 0;

	return SPDK_POLLER_IDLE;
}

static int
_congctrl_cong_update(void *arg)
{
	struct congctrl_io_channel *congctrl_ch = arg;
	uint64_t ticks = spdk_get_ticks();
	int submissions = 0;

	_congctrl_cong_token_refill(&congctrl_ch->rd_cong, ticks);
	_congctrl_cong_token_refill(&congctrl_ch->wr_cong, ticks);

	submissions += _resubmit_io_tailq(&congctrl_ch->read_io_wait_queue);
	submissions += _resubmit_io_tailq(&congctrl_ch->write_io_wait_queue);

	return submissions == 0 ? SPDK_POLLER_IDLE : SPDK_POLLER_BUSY;
}

/* Completion callback for IO that were issued from this bdev. The original bdev_io
 * is passed in as an arg so we'll complete that one with the appropriate status
 * and then free the one that this module issued.
 */
static void
_congctrl_complete_io(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;
	struct congctrl_bdev_io *io_ctx = (struct congctrl_bdev_io *)orig_io->driver_ctx;
	struct congctrl_io_channel *congctrl_ch = spdk_io_channel_get_ctx(io_ctx->ch);
	struct congctrl_cong *cong;	
	int res;
	uint64_t iolen;

	io_ctx->status = success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;
	spdk_bdev_free_io(bdev_io);

	if (io_ctx->type == LATENCY_NONE) {
		spdk_bdev_io_complete(orig_io, io_ctx->status);
		return;
	} else {
		io_ctx->completion_tick = spdk_get_ticks();
		if (io_ctx->type == LATENCY_READ) {
			cong = &congctrl_ch->rd_cong;
		} else {
			cong = &congctrl_ch->wr_cong;
		}
	}
	iolen = orig_io->u.bdev.num_blocks * orig_io->bdev->blocklen;
	_congctrl_cong_token_done(cong, io_ctx->completion_tick, iolen);
	if (spdk_likely(io_ctx->status == SPDK_BDEV_IO_STATUS_SUCCESS)) {
		res = _congctrl_cong_latency_update(cong,
					iolen, io_ctx->completion_tick - io_ctx->submit_tick);
		//SPDK_NOTICELOG("_congctrl_cong_latency_update: iolen:%lu res:%d tick:%lu\n",
		//					iolen, res, io_ctx->completion_tick - io_ctx->submit_tick);
		_congctrl_cong_token_update(cong, res, iolen);
	}
	spdk_bdev_io_complete(orig_io, io_ctx->status);
	return;
}

static void
vbdev_congctrl_resubmit_io(void *arg)
{
	struct spdk_bdev_io *bdev_io = (struct spdk_bdev_io *)arg;
	struct congctrl_bdev_io *io_ctx = (struct congctrl_bdev_io *)bdev_io->driver_ctx;

	vbdev_congctrl_submit_request(io_ctx->ch, bdev_io);
}

static int
vbdev_congctrl_resubmit_cong_io(void *arg)
{
	struct spdk_bdev_io *bdev_io = (struct spdk_bdev_io *)arg;
	struct congctrl_bdev_io *io_ctx = (struct congctrl_bdev_io *)bdev_io->driver_ctx;
	struct congctrl_io_channel *congctrl_ch = spdk_io_channel_get_ctx(io_ctx->ch);
	struct congctrl_cong *cong = NULL;
	uint64_t iolen = bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen;

	if (io_ctx->type == LATENCY_READ) {
		cong = &congctrl_ch->rd_cong;
	} else if (io_ctx->type == LATENCY_WRITE) {
		cong = &congctrl_ch->wr_cong;
	}

	if (cong && cong->tokens < iolen) {
	//if (cong->rate != 0 && cong->tokens < cong->rate + iolen) {
		//printf("EAGAIN: %ld/%lu\n", cong->rate, cong->tokens);
		return -EAGAIN;
	}

	vbdev_congctrl_submit_request(io_ctx->ch, bdev_io);
	return 0;
}

static void
vbdev_congctrl_queue_io(struct spdk_bdev_io *bdev_io)
{
	struct congctrl_bdev_io *io_ctx = (struct congctrl_bdev_io *)bdev_io->driver_ctx;
	struct congctrl_io_channel *congctrl_ch = spdk_io_channel_get_ctx(io_ctx->ch);
	int rc;

	io_ctx->bdev_io_wait.bdev = bdev_io->bdev;
	io_ctx->bdev_io_wait.cb_fn = vbdev_congctrl_resubmit_io;
	io_ctx->bdev_io_wait.cb_arg = bdev_io;

	rc = spdk_bdev_queue_io_wait(bdev_io->bdev, congctrl_ch->base_ch, &io_ctx->bdev_io_wait);
	if (rc != 0) {
		SPDK_ERRLOG("Queue io failed in vbdev_congctrl_queue_io, rc=%d.\n", rc);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static void
vbdev_congctrl_cong_io_wait(struct spdk_bdev_io *bdev_io)
{
	struct congctrl_bdev_io *io_ctx = (struct congctrl_bdev_io *)bdev_io->driver_ctx;
	struct congctrl_io_channel *congctrl_ch = spdk_io_channel_get_ctx(io_ctx->ch);

	io_ctx->congctrl_io_wait.cb_fn = vbdev_congctrl_resubmit_cong_io;
	io_ctx->congctrl_io_wait.cb_arg = bdev_io;

	if (io_ctx->type == LATENCY_READ) {
		TAILQ_INSERT_TAIL(&congctrl_ch->read_io_wait_queue, &io_ctx->congctrl_io_wait, link);
	} else if (io_ctx->type == LATENCY_WRITE) {
		TAILQ_INSERT_TAIL(&congctrl_ch->write_io_wait_queue, &io_ctx->congctrl_io_wait, link);
	} else {
		SPDK_ERRLOG("invalid io_type in vbdev_congctrl_cong_io_wait\n");
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static void
congctrl_read_get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io, bool success)
{
	struct vbdev_congctrl *congctrl_node = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_congctrl,
					 congctrl_bdev);
	struct congctrl_io_channel *congctrl_ch = spdk_io_channel_get_ctx(ch);
	struct congctrl_bdev_io *io_ctx = (struct congctrl_bdev_io *)bdev_io->driver_ctx;
	int rc;

	if (!success) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	rc = spdk_bdev_readv_blocks(congctrl_node->base_desc, congctrl_ch->base_ch, bdev_io->u.bdev.iovs,
				    bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.offset_blocks,
				    bdev_io->u.bdev.num_blocks, _congctrl_complete_io,
				    bdev_io);

	if (rc == -ENOMEM) {
		SPDK_ERRLOG("No memory, start to queue io for delay.\n");
		vbdev_congctrl_queue_io(bdev_io);
	} else if (rc != 0) {
		SPDK_ERRLOG("ERROR on bdev_io submission!\n");
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	} else {
		io_ctx->submit_tick = spdk_get_ticks();
	}
}

static void
vbdev_congctrl_reset_dev(struct spdk_io_channel_iter *i, int status)
{
	struct spdk_bdev_io *bdev_io = spdk_io_channel_iter_get_ctx(i);
	struct congctrl_bdev_io *io_ctx = (struct congctrl_bdev_io *)bdev_io->driver_ctx;
	struct congctrl_io_channel *congctrl_ch = spdk_io_channel_get_ctx(io_ctx->ch);
	struct vbdev_congctrl *congctrl_node = spdk_io_channel_iter_get_io_device(i);
	int rc;

	rc = spdk_bdev_reset(congctrl_node->base_desc, congctrl_ch->base_ch,
			     _congctrl_complete_io, bdev_io);

	if (rc == -ENOMEM) {
		SPDK_ERRLOG("No memory, start to queue io for delay.\n");
		vbdev_congctrl_queue_io(bdev_io);
	} else if (rc != 0) {
		SPDK_ERRLOG("ERROR on bdev_io submission!\n");
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static void
_abort_all_congctrled_io(void *arg)
{
	STAILQ_HEAD(, congctrl_bdev_io) *head = arg;
	struct congctrl_bdev_io *io_ctx, *tmp;

	STAILQ_FOREACH_SAFE(io_ctx, head, link, tmp) {
		STAILQ_REMOVE(head, io_ctx, congctrl_bdev_io, link);
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(io_ctx), SPDK_BDEV_IO_STATUS_ABORTED);
	}
}

static void
vbdev_congctrl_reset_channel(struct spdk_io_channel_iter *i)
{
	struct spdk_io_channel *ch = spdk_io_channel_iter_get_channel(i);
	struct congctrl_io_channel *congctrl_ch = spdk_io_channel_get_ctx(ch);

	_abort_all_congctrled_io(&congctrl_ch->read_io_wait_queue);
	_abort_all_congctrled_io(&congctrl_ch->write_io_wait_queue);

	spdk_for_each_channel_continue(i, 0);
}

static bool
abort_congctrled_io(void *_head, struct spdk_bdev_io *bio_to_abort)
{
	STAILQ_HEAD(, congctrl_bdev_io) *head = _head;
	struct congctrl_bdev_io *io_ctx_to_abort = (struct congctrl_bdev_io *)bio_to_abort->driver_ctx;
	struct congctrl_bdev_io *io_ctx;

	STAILQ_FOREACH(io_ctx, head, link) {
		if (io_ctx == io_ctx_to_abort) {
			STAILQ_REMOVE(head, io_ctx_to_abort, congctrl_bdev_io, link);
			spdk_bdev_io_complete(bio_to_abort, SPDK_BDEV_IO_STATUS_ABORTED);
			return true;
		}
	}

	return false;
}

static int
vbdev_congctrl_abort(struct vbdev_congctrl *congctrl_node, struct congctrl_io_channel *congctrl_ch,
		  struct spdk_bdev_io *bdev_io)
{
	struct spdk_bdev_io *bio_to_abort = bdev_io->u.abort.bio_to_abort;

	if (abort_congctrled_io(&congctrl_ch->read_io_wait_queue, bio_to_abort) ||
	    abort_congctrled_io(&congctrl_ch->write_io_wait_queue, bio_to_abort)) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
		return 0;
	}

	return spdk_bdev_abort(congctrl_node->base_desc, congctrl_ch->base_ch, bio_to_abort,
			       _congctrl_complete_io, bdev_io);
}

static void
vbdev_congctrl_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct vbdev_congctrl *congctrl_node = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_congctrl, congctrl_bdev);
	struct congctrl_io_channel *congctrl_ch = spdk_io_channel_get_ctx(ch);
	struct congctrl_bdev_io *io_ctx = (struct congctrl_bdev_io *)bdev_io->driver_ctx;
	int rc = 0;

	io_ctx->ch = ch;
	io_ctx->type = LATENCY_NONE;
	
	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		io_ctx->type = LATENCY_READ;
		if ((rc = _congctrl_cong_token_get(congctrl_ch, io_ctx->type,
						bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen)) < 0) {
			break;
		} else {
			rc = 0;
			spdk_bdev_io_get_buf(bdev_io, congctrl_read_get_buf_cb,
						bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		}
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		io_ctx->type = LATENCY_WRITE;
		if ((rc = _congctrl_cong_token_get(congctrl_ch, io_ctx->type,
						bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen)) < 0) {
			break;
		} else {
			rc = spdk_bdev_writev_blocks(congctrl_node->base_desc, congctrl_ch->base_ch, bdev_io->u.bdev.iovs,
							bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.offset_blocks,
							bdev_io->u.bdev.num_blocks, _congctrl_complete_io,
							bdev_io);
		}
		break;
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		io_ctx->type = LATENCY_WRITE;
		if ((rc = _congctrl_cong_token_get(congctrl_ch, io_ctx->type,
						bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen)) < 0) {
			break;
		} else {
			rc = spdk_bdev_write_zeroes_blocks(congctrl_node->base_desc, congctrl_ch->base_ch,
							bdev_io->u.bdev.offset_blocks,
							bdev_io->u.bdev.num_blocks,
							_congctrl_complete_io, bdev_io);
		}
		break;
	case SPDK_BDEV_IO_TYPE_ZONE_APPEND:
		io_ctx->type = LATENCY_WRITE;
		if ((rc = _congctrl_cong_token_get(congctrl_ch, io_ctx->type,
						bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen)) < 0) {
			break;
		} else {
			rc = spdk_bdev_zone_appendv(congctrl_node->base_desc, congctrl_ch->base_ch, bdev_io->u.bdev.iovs,
							bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.offset_blocks,
							bdev_io->u.bdev.num_blocks, _congctrl_complete_io,
							bdev_io);
		}
		break;
	case SPDK_BDEV_IO_TYPE_GET_ZONE_INFO:
		rc = spdk_bdev_get_zone_info(congctrl_node->base_desc, congctrl_ch->base_ch,
						bdev_io->u.zone_mgmt.zone_id, bdev_io->u.zone_mgmt.num_zones,
						bdev_io->u.zone_mgmt.buf, _congctrl_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT:
		rc = spdk_bdev_zone_management(congctrl_node->base_desc, congctrl_ch->base_ch,
						bdev_io->u.zone_mgmt.zone_id, bdev_io->u.zone_mgmt.zone_action,
						_congctrl_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_UNMAP:
		rc = spdk_bdev_unmap_blocks(congctrl_node->base_desc, congctrl_ch->base_ch,
					    bdev_io->u.bdev.offset_blocks,
					    bdev_io->u.bdev.num_blocks,
					    _congctrl_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_FLUSH:
		rc = spdk_bdev_flush_blocks(congctrl_node->base_desc, congctrl_ch->base_ch,
					    bdev_io->u.bdev.offset_blocks,
					    bdev_io->u.bdev.num_blocks,
					    _congctrl_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_RESET:
		/* During reset, the generic bdev layer aborts all new I/Os and queues all new resets.
		 * Hence we can simply abort all I/Os delayed to complete.
		 */
		spdk_for_each_channel(congctrl_node, vbdev_congctrl_reset_channel, bdev_io,
				      vbdev_congctrl_reset_dev);
		break;
	case SPDK_BDEV_IO_TYPE_ABORT:
		rc = vbdev_congctrl_abort(congctrl_node, congctrl_ch, bdev_io);
		break;
	default:
		SPDK_ERRLOG("congctrl: unknown I/O type %d\n", bdev_io->type);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	if (rc == -EAGAIN) {
		//vbdev_congctrl_cong_io_wait(bdev_io);
		bdev_io->internal.error.aio_result = rc;
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_AIO_ERROR);
	} else if (rc == -ENOMEM) {
		SPDK_ERRLOG("No memory, start to queue io for congctrl.\n");
		vbdev_congctrl_queue_io(bdev_io);
	} else if (rc != 0) {
		SPDK_ERRLOG("ERROR on bdev_io submission!\n");
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	} else {
		io_ctx->submit_tick = spdk_get_ticks();
	}
}

static bool
vbdev_congctrl_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	struct vbdev_congctrl *congctrl_node = (struct vbdev_congctrl *)ctx;

	if (io_type == SPDK_BDEV_IO_TYPE_ZCOPY) {
		return false;
	} else {
		return spdk_bdev_io_type_supported(congctrl_node->base_bdev, io_type);
	}
}

static struct spdk_io_channel *
vbdev_congctrl_get_io_channel(void *ctx)
{
	struct vbdev_congctrl *congctrl_node = (struct vbdev_congctrl *)ctx;
	struct spdk_io_channel *congctrl_ch = NULL;

	congctrl_ch = spdk_get_io_channel(congctrl_node);

	return congctrl_ch;
}

static void
_congctrl_write_conf_values(struct vbdev_congctrl *congctrl_node, struct spdk_json_write_ctx *w)
{
	spdk_json_write_named_string(w, "name", spdk_bdev_get_name(&congctrl_node->congctrl_bdev));
	spdk_json_write_named_string(w, "base_bdev_name", spdk_bdev_get_name(congctrl_node->base_bdev));
	spdk_json_write_named_uint64(w, "upper_read_latency",
				    congctrl_node->upper_read_latency * SPDK_SEC_TO_USEC / spdk_get_ticks_hz());
	spdk_json_write_named_uint64(w, "lower_read_latency",
				    congctrl_node->lower_read_latency * SPDK_SEC_TO_USEC / spdk_get_ticks_hz());
	spdk_json_write_named_uint64(w, "upper_write_latency",
				    congctrl_node->upper_write_latency * SPDK_SEC_TO_USEC / spdk_get_ticks_hz());
	spdk_json_write_named_uint64(w, "lower_write_latency",
				    congctrl_node->lower_write_latency * SPDK_SEC_TO_USEC / spdk_get_ticks_hz());
}

static int
vbdev_congctrl_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct vbdev_congctrl *congctrl_node = (struct vbdev_congctrl *)ctx;

	spdk_json_write_name(w, "congctrl");
	spdk_json_write_object_begin(w);
	_congctrl_write_conf_values(congctrl_node, w);
	spdk_json_write_object_end(w);

	return 0;
}

/* This is used to generate JSON that can configure this module to its current state. */
static int
vbdev_congctrl_config_json(struct spdk_json_write_ctx *w)
{
	struct vbdev_congctrl *congctrl_node;

	TAILQ_FOREACH(congctrl_node, &g_congctrl_nodes, link) {
		spdk_json_write_object_begin(w);
		spdk_json_write_named_string(w, "method", "bdev_congctrl_create");
		spdk_json_write_named_object_begin(w, "params");
		_congctrl_write_conf_values(congctrl_node, w);
		spdk_json_write_object_end(w);
		spdk_json_write_object_end(w);
	}
	return 0;
}

/* We provide this callback for the SPDK channel code to create a channel using
 * the channel struct we provided in our module get_io_channel() entry point. Here
 * we get and save off an underlying base channel of the device below us so that
 * we can communicate with the base bdev on a per channel basis.  If we needed
 * our own poller for this vbdev, we'd register it here.
 */
static int
congctrl_bdev_ch_create_cb(void *io_device, void *ctx_buf)
{
	struct congctrl_io_channel *congctrl_ch = ctx_buf;
	struct vbdev_congctrl *congctrl_node = io_device;

	TAILQ_INIT(&congctrl_ch->read_io_wait_queue);
	TAILQ_INIT(&congctrl_ch->write_io_wait_queue);

	_congctrl_cong_init(&congctrl_ch->rd_cong,
			congctrl_node->upper_read_latency, congctrl_node->upper_read_latency);
	_congctrl_cong_init(&congctrl_ch->wr_cong,
			congctrl_node->upper_write_latency, congctrl_node->upper_write_latency);
	congctrl_ch->io_poller = SPDK_POLLER_REGISTER(_congctrl_cong_update, congctrl_ch, 100);
	congctrl_ch->top_poller = SPDK_POLLER_REGISTER(_congctrl_cong_top, congctrl_ch, 1000000);
	congctrl_ch->base_ch = spdk_bdev_get_io_channel(congctrl_node->base_desc);
	congctrl_ch->rand_seed = time(NULL);

	return 0;
}

/* We provide this callback for the SPDK channel code to destroy a channel
 * created with our create callback. We just need to undo anything we did
 * when we created. If this bdev used its own poller, we'd unregsiter it here.
 */
static void
congctrl_bdev_ch_destroy_cb(void *io_device, void *ctx_buf)
{
	struct congctrl_io_channel *congctrl_ch = ctx_buf;

	spdk_poller_unregister(&congctrl_ch->top_poller);
	spdk_poller_unregister(&congctrl_ch->io_poller);
	spdk_put_io_channel(congctrl_ch->base_ch);
}

/* Create the congctrl association from the bdev and vbdev name and insert
 * on the global list. */
static int
vbdev_congctrl_insert_association(const char *bdev_name, const char *vbdev_name,
			       uint64_t upper_read_latency, uint64_t lower_read_latency,
			       uint64_t upper_write_latency, uint64_t lower_write_latency)
{
	struct bdev_association *assoc;

	TAILQ_FOREACH(assoc, &g_bdev_associations, link) {
		if (strcmp(vbdev_name, assoc->vbdev_name) == 0) {
			SPDK_ERRLOG("congctrl bdev %s already exists\n", vbdev_name);
			return -EEXIST;
		}
	}

	assoc = calloc(1, sizeof(struct bdev_association));
	if (!assoc) {
		SPDK_ERRLOG("could not allocate bdev_association\n");
		return -ENOMEM;
	}

	assoc->bdev_name = strdup(bdev_name);
	if (!assoc->bdev_name) {
		SPDK_ERRLOG("could not allocate assoc->bdev_name\n");
		free(assoc);
		return -ENOMEM;
	}

	assoc->vbdev_name = strdup(vbdev_name);
	if (!assoc->vbdev_name) {
		SPDK_ERRLOG("could not allocate assoc->vbdev_name\n");
		free(assoc->bdev_name);
		free(assoc);
		return -ENOMEM;
	}

	assoc->upper_read_latency = upper_read_latency;
	assoc->lower_read_latency = lower_read_latency;
	assoc->upper_write_latency = upper_write_latency;
	assoc->lower_write_latency = lower_write_latency;

	TAILQ_INSERT_TAIL(&g_bdev_associations, assoc, link);

	return 0;
}

int
vbdev_congctrl_update_latency_value(char *congctrl_name, uint64_t latency_upper,
		  uint64_t latency_lower, enum congctrl_lat_type type)
{
	struct spdk_bdev *congctrl_bdev;
	struct vbdev_congctrl *congctrl_node;
	uint64_t ticks_mhz = spdk_get_ticks_hz() / SPDK_SEC_TO_USEC;

	congctrl_bdev = spdk_bdev_get_by_name(congctrl_name);
	if (congctrl_bdev == NULL) {
		return -ENODEV;
	} else if (congctrl_bdev->module != &congctrl_if) {
		return -EINVAL;
	}

	congctrl_node = SPDK_CONTAINEROF(congctrl_bdev, struct vbdev_congctrl, congctrl_bdev);

	switch (type) {
	case LATENCY_READ:
		congctrl_node->upper_read_latency = ticks_mhz * latency_upper;
		congctrl_node->lower_read_latency = ticks_mhz * latency_lower;
		break;
	case LATENCY_WRITE:
		congctrl_node->upper_write_latency = ticks_mhz * latency_upper;
		congctrl_node->lower_write_latency = ticks_mhz * latency_lower;
		break;
	default:
		return -EINVAL;
	}

	return 0;
}

static int
vbdev_congctrl_init(void)
{
	/* Not allowing for .ini style configuration. */
	return 0;
}

static void
vbdev_congctrl_finish(void)
{
	struct bdev_association *assoc;

	while ((assoc = TAILQ_FIRST(&g_bdev_associations))) {
		TAILQ_REMOVE(&g_bdev_associations, assoc, link);
		free(assoc->bdev_name);
		free(assoc->vbdev_name);
		free(assoc);
	}
}

static int
vbdev_congctrl_get_ctx_size(void)
{
	return sizeof(struct congctrl_bdev_io);
}

static int
vbdev_congctrl_get_memory_domains(void *ctx, struct spdk_memory_domain **domains, int array_size)
{
	struct vbdev_congctrl *congctrl_node = (struct vbdev_congctrl *)ctx;

	/* Delay bdev doesn't work with data buffers, so it supports any memory domain used by base_bdev */
	return spdk_bdev_get_memory_domains(congctrl_node->base_bdev, domains, array_size);
}

/* When we register our bdev this is how we specify our entry points. */
static const struct spdk_bdev_fn_table vbdev_congctrl_fn_table = {
	.destruct		= vbdev_congctrl_destruct,
	.submit_request		= vbdev_congctrl_submit_request,
	.io_type_supported	= vbdev_congctrl_io_type_supported,
	.get_io_channel		= vbdev_congctrl_get_io_channel,
	.dump_info_json		= vbdev_congctrl_dump_info_json,
	.write_config_json	= NULL,
	.get_memory_domains	= vbdev_congctrl_get_memory_domains,
};

static void
vbdev_congctrl_base_bdev_hotremove_cb(struct spdk_bdev *bdev_find)
{
	struct vbdev_congctrl *congctrl_node, *tmp;

	TAILQ_FOREACH_SAFE(congctrl_node, &g_congctrl_nodes, link, tmp) {
		if (bdev_find == congctrl_node->base_bdev) {
			spdk_bdev_unregister(&congctrl_node->congctrl_bdev, NULL, NULL);
		}
	}
}

/* Called when the underlying base bdev triggers asynchronous event such as bdev removal. */
static void
vbdev_congctrl_base_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
			       void *event_ctx)
{
	switch (type) {
	case SPDK_BDEV_EVENT_REMOVE:
		vbdev_congctrl_base_bdev_hotremove_cb(bdev);
		break;
	default:
		SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
		break;
	}
}

/* Create and register the congctrl vbdev if we find it in our list of bdev names.
 * This can be called either by the examine path or RPC method.
 */
static int
vbdev_congctrl_register(const char *bdev_name)
{
	struct bdev_association *assoc;
	struct vbdev_congctrl *congctrl_node;
	struct spdk_bdev *bdev;
	uint64_t ticks_mhz = spdk_get_ticks_hz() / SPDK_SEC_TO_USEC;
	int rc = 0;

	/* Check our list of names from config versus this bdev and if
	 * there's a match, create the congctrl_node & bdev accordingly.
	 */
	TAILQ_FOREACH(assoc, &g_bdev_associations, link) {
		if (strcmp(assoc->bdev_name, bdev_name) != 0) {
			continue;
		}

		congctrl_node = calloc(1, sizeof(struct vbdev_congctrl));
		if (!congctrl_node) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate congctrl_node\n");
			break;
		}
		congctrl_node->congctrl_bdev.name = strdup(assoc->vbdev_name);
		if (!congctrl_node->congctrl_bdev.name) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate congctrl_bdev name\n");
			free(congctrl_node);
			break;
		}
		congctrl_node->congctrl_bdev.product_name = "congctrl";

		/* The base bdev that we're attaching to. */
		rc = spdk_bdev_open_ext(bdev_name, true, vbdev_congctrl_base_bdev_event_cb,
					NULL, &congctrl_node->base_desc);
		if (rc) {
			if (rc != -ENODEV) {
				SPDK_ERRLOG("could not open bdev %s\n", bdev_name);
			}
			free(congctrl_node->congctrl_bdev.name);
			free(congctrl_node);
			break;
		}

		bdev = spdk_bdev_desc_get_bdev(congctrl_node->base_desc);
		congctrl_node->base_bdev = bdev;

		congctrl_node->congctrl_bdev.write_cache = bdev->write_cache;
		congctrl_node->congctrl_bdev.required_alignment = bdev->required_alignment;
		congctrl_node->congctrl_bdev.optimal_io_boundary = bdev->optimal_io_boundary;
		congctrl_node->congctrl_bdev.blocklen = bdev->blocklen;
		congctrl_node->congctrl_bdev.blockcnt = bdev->blockcnt;

		if (spdk_bdev_is_zoned(bdev)) {
			congctrl_node->congctrl_bdev.zoned = true;
			congctrl_node->congctrl_bdev.zone_size = bdev->zone_size;
			congctrl_node->congctrl_bdev.max_zone_append_size = bdev->max_zone_append_size;
			congctrl_node->congctrl_bdev.max_open_zones = bdev->max_open_zones;
			congctrl_node->congctrl_bdev.max_active_zones = bdev->max_active_zones;
			congctrl_node->congctrl_bdev.optimal_open_zones = bdev->optimal_open_zones;
		}

		congctrl_node->congctrl_bdev.ctxt = congctrl_node;
		congctrl_node->congctrl_bdev.fn_table = &vbdev_congctrl_fn_table;
		congctrl_node->congctrl_bdev.module = &congctrl_if;

		/* Store the number of ticks you need to add to get the I/O expiration time. */
		congctrl_node->upper_read_latency = ticks_mhz * assoc->upper_read_latency;
		congctrl_node->lower_read_latency = ticks_mhz * assoc->lower_read_latency;
		congctrl_node->upper_write_latency = ticks_mhz * assoc->upper_write_latency;
		congctrl_node->lower_write_latency = ticks_mhz * assoc->lower_write_latency;

		spdk_io_device_register(congctrl_node, congctrl_bdev_ch_create_cb, congctrl_bdev_ch_destroy_cb,
					sizeof(struct congctrl_io_channel),
					assoc->vbdev_name);

		/* Save the thread where the base device is opened */
		congctrl_node->thread = spdk_get_thread();

		rc = spdk_bdev_module_claim_bdev(bdev, congctrl_node->base_desc, congctrl_node->congctrl_bdev.module);
		if (rc) {
			SPDK_ERRLOG("could not claim bdev %s\n", bdev_name);
			goto error_close;
		}

		rc = spdk_bdev_register(&congctrl_node->congctrl_bdev);
		if (rc) {
			SPDK_ERRLOG("could not register congctrl_bdev\n");
			spdk_bdev_module_release_bdev(congctrl_node->base_bdev);
			goto error_close;
		}

		TAILQ_INSERT_TAIL(&g_congctrl_nodes, congctrl_node, link);
	}

	return rc;

error_close:
	spdk_bdev_close(congctrl_node->base_desc);
	spdk_io_device_unregister(congctrl_node, NULL);
	free(congctrl_node->congctrl_bdev.name);
	free(congctrl_node);
	return rc;
}

int
create_congctrl_disk(const char *bdev_name, const char *vbdev_name, uint64_t upper_read_latency,
		  uint64_t lower_read_latency, uint64_t upper_write_latency, uint64_t lower_write_latency)
{
	struct bdev_association *assoc;
	int rc = 0;

	if (upper_read_latency < lower_read_latency || upper_write_latency < lower_write_latency) {
		SPDK_ERRLOG("Unable to create a congctrl bdev where upper latency is less than lower latency.\n");
		return -EINVAL;
	}

	rc = vbdev_congctrl_insert_association(bdev_name, vbdev_name, upper_read_latency, lower_read_latency,
					    upper_write_latency, lower_write_latency);
	if (rc) {
		return rc;
	}

	rc = vbdev_congctrl_register(bdev_name);
	if (rc == -ENODEV) {
		/* This is not an error, we tracked the name above and it still
		 * may show up later.
		 */
		SPDK_NOTICELOG("vbdev creation deferred pending base bdev arrival\n");
		rc = 0;
	} else if (rc != 0) {
		TAILQ_FOREACH(assoc, &g_bdev_associations, link) {
			if (strcmp(assoc->vbdev_name, vbdev_name) == 0) {
				TAILQ_REMOVE(&g_bdev_associations, assoc, link);
				free(assoc->bdev_name);
				free(assoc->vbdev_name);
				free(assoc);
				break;
			}
		}
	}

	return rc;
}

void
delete_congctrl_disk(struct spdk_bdev *bdev, spdk_bdev_unregister_cb cb_fn, void *cb_arg)
{
	struct bdev_association *assoc;

	if (!bdev || bdev->module != &congctrl_if) {
		cb_fn(cb_arg, -ENODEV);
		return;
	}

	TAILQ_FOREACH(assoc, &g_bdev_associations, link) {
		if (strcmp(assoc->vbdev_name, bdev->name) == 0) {
			SPDK_NOTICELOG("association of vbdev %s deleted\n", assoc->vbdev_name);
			TAILQ_REMOVE(&g_bdev_associations, assoc, link);
			free(assoc->bdev_name);
			free(assoc->vbdev_name);
			free(assoc);
			break;
		}
	}

	spdk_bdev_unregister(bdev, cb_fn, cb_arg);
}

static void
vbdev_congctrl_examine(struct spdk_bdev *bdev)
{
	vbdev_congctrl_register(bdev->name);

	spdk_bdev_module_examine_done(&congctrl_if);
}

SPDK_LOG_REGISTER_COMPONENT(vbdev_congctrl)

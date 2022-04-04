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

#include "vbdev_congctrl_internal.h"
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

/* congctrl associative list to be used in examine */
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

/* congctrl_ns associative list to be used in examine */
struct ns_association {
	char			*ctrl_name;
	char			*ns_name;

	// Namespace specific parameters
	uint32_t	zone_array_size;
	uint32_t	stripe_size;
	uint32_t	block_align;

	TAILQ_ENTRY(ns_association)	link;
};
static TAILQ_HEAD(, ns_association) g_ns_associations = TAILQ_HEAD_INITIALIZER(
			g_ns_associations);


static TAILQ_HEAD(, vbdev_congctrl) g_congctrl_nodes = TAILQ_HEAD_INITIALIZER(g_congctrl_nodes);

static void vbdev_congctrl_ns_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io);

/* Callback for unregistering the IO device. */
static void
_ns_unregister_cb(void *io_device)
{
	struct vbdev_congctrl_ns *congctrl_ns  = io_device;

	/* Done with this congctrl_ns. */
	free(congctrl_ns->congctrl_ns_bdev.name);
	free(congctrl_ns);
}

static void
_device_unregister_cb(void *io_device)
{
	struct vbdev_congctrl *congctrl_node  = io_device;

	/* Done with this congctrl_node. */
	free(congctrl_node->mgmt_bdev.name);
	free(congctrl_node);
}

static int
vbdev_congctrl_ns_destruct(void *ctx)
{
	struct vbdev_congctrl_ns *congctrl_ns = (struct vbdev_congctrl_ns *)ctx;
	struct vbdev_congctrl    *congctrl_node = (struct vbdev_congctrl *)congctrl_ns->ctrl;
	/* It is important to follow this exact sequence of steps for destroying
	 * a vbdev...
	 */

	TAILQ_REMOVE(&congctrl_node->ns, congctrl_ns, link);

	/* Unregister the io_device. */
	spdk_io_device_unregister(congctrl_ns, _ns_unregister_cb);

	return 0;
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
	struct vbdev_congctrl_ns *congctrl_ns, *tmp_ns;

	/* It is important to follow this exact sequence of steps for destroying
	 * a vbdev...
	 */
	TAILQ_FOREACH_SAFE(congctrl_ns, &congctrl_node->ns, link, tmp_ns) {
		spdk_bdev_unregister(&congctrl_ns->congctrl_ns_bdev, NULL, NULL);
	}


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
_congctrl_cong_init(struct congctrl_sched *sched, struct vbdev_congctrl *congctrl_node)
{
	sched->ewma_ticks = 0;
	sched->last_update_tsc = spdk_get_ticks();
	sched->rate = (1000UL) * 1024 * 1024;
	//sched->rate = 0;
	sched->tokens = 128*1024;	// TODO: to be Zone-MDTS of the device
	sched->max_bucket_size = 16*128*1024;
}

static inline int
_congctrl_sched_latency_check(struct congctrl_sched *sched, uint64_t iolen, uint64_t latency_ticks)
{
	// TODO
	// 1. Get the current I/O completion rate
	// 2. Check the last I/O latency is in the range
	// 3. If it detects a write cache overflow, enable DRR scheduling and set the rate limit

	return 0;
}

static int
_resubmit_io_tailq(void *arg)
{
	TAILQ_HEAD(, congctrl_bdev_io) *head = arg;
	struct congctrl_bdev_io *entry, *tmp;
	int submissions = 0;

	// TODO:
	return submissions;
}

static int
_congctrl_cong_top(void *arg)
{
	struct congctrl_io_channel *congctrl_ch = arg;

	return SPDK_POLLER_IDLE;
}

static int
_congctrl_cong_update(void *arg)
{
	struct congctrl_io_channel *congctrl_ch = arg;
	uint64_t ticks = spdk_get_ticks();
	int submissions = 0;

	//TODO: Update DRR scheduling and enqueue available I/Os to the sliding windows scheduling
	//submissions += _resubmit_io_tailq(&congctrl_ch->read_io_wait_queue);
	//submissions += _resubmit_io_tailq(&congctrl_ch->write_io_wait_queue);

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
	uint64_t iolen = orig_io->u.bdev.num_blocks * orig_io->bdev->blocklen;

	spdk_bdev_free_io(bdev_io);
	if (!success) {
		io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
	}

	if (io_ctx->type == CONGCTRL_IO_WRITE || success) {
		// TODO: Check latency and throughput for write I/O on every successful write completion.
		//       If the latency is higher than actual throughput, it implies write cache overflow.
		io_ctx->completion_tick = spdk_get_ticks();

		// TODO: close zone (logically) if write I/O completes at the end
	}

	// TODO: Move sliding window
	//_congctrl_slidewin_(io_ctx->cong, io_ctx->completion_tick, iolen);

	assert(io_ctx->outstanding_split_ios);
	io_ctx->outstanding_split_ios--;
	printf("spdk_bdev_io_complete: rc:%d out_ios:%u remain_blocks:%llu\n", io_ctx->status, io_ctx->outstanding_split_ios, io_ctx->remain_blocks);
	if (io_ctx->outstanding_split_ios == 0 && io_ctx->remain_blocks == 0) {
		spdk_bdev_io_complete(orig_io,
			 (io_ctx->status == SPDK_BDEV_IO_STATUS_FAILED) ? SPDK_BDEV_IO_STATUS_FAILED: SPDK_BDEV_IO_STATUS_SUCCESS);
	}
	return;
}

/// Completion callback for mgmt commands that were issued from this bdev.
static void
_congctrl_complete_mgmt(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;

	spdk_bdev_free_io(bdev_io);
	spdk_bdev_io_complete(orig_io, success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED);
	return;
}

static void
vbdev_congctrl_resubmit_io(void *arg)
{
	struct spdk_bdev_io *bdev_io = (struct spdk_bdev_io *)arg;
	struct congctrl_bdev_io *io_ctx = (struct congctrl_bdev_io *)bdev_io->driver_ctx;

	vbdev_congctrl_ns_submit_request(io_ctx->ch, bdev_io);
}

static int
vbdev_congctrl_queue_io(struct spdk_bdev_io *bdev_io)
{
	struct congctrl_bdev_io *io_ctx = (struct congctrl_bdev_io *)bdev_io->driver_ctx;
	struct congctrl_io_channel *congctrl_ch = spdk_io_channel_get_ctx(io_ctx->ch);

	io_ctx->bdev_io_wait.bdev = bdev_io->bdev;
	io_ctx->bdev_io_wait.cb_fn = vbdev_congctrl_resubmit_io;
	io_ctx->bdev_io_wait.cb_arg = bdev_io;

	return spdk_bdev_queue_io_wait(bdev_io->bdev, congctrl_ch->base_ch, &io_ctx->bdev_io_wait);
}

static void
_vbdev_congctrl_slidewin_io_wait(struct spdk_bdev_io *bdev_io)
{
	struct congctrl_bdev_io *io_ctx = (struct congctrl_bdev_io *)bdev_io->driver_ctx;
	struct congctrl_io_channel *congctrl_ch = spdk_io_channel_get_ctx(io_ctx->ch);

	TAILQ_INSERT_TAIL(&congctrl_ch->slidewin_wait_queue, io_ctx, link);
}

static void
_vbdev_congctrl_drr_io_wait(struct spdk_bdev_io *bdev_io)
{
	struct congctrl_bdev_io *io_ctx = (struct congctrl_bdev_io *)bdev_io->driver_ctx;
	struct congctrl_io_channel *congctrl_ch = spdk_io_channel_get_ctx(io_ctx->ch);

	TAILQ_INSERT_TAIL(&congctrl_ch->write_drr_queue, io_ctx, link);
}

static void
//_congctrl_ns_read_get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io, bool success)
_congctrl_ns_rw_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io, bool success)
{
	struct vbdev_congctrl_ns *congctrl_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_congctrl_ns,
					 congctrl_ns_bdev);
	struct vbdev_congctrl *congctrl_node = congctrl_ns->ctrl;
	struct congctrl_io_channel *congctrl_ch = spdk_io_channel_get_ctx(ch);
	struct congctrl_bdev_io *io_ctx = (struct congctrl_bdev_io *)bdev_io->driver_ctx;
	int rc = 0;
	uint64_t lzslba;		// Logical Zone Start LBA
	uint64_t lz_stripe_idx;
	uint64_t zone_array_id, lz_offset_blocks;
	uint64_t remain_stripe_blocks;
	uint64_t io_offset_blocks;
	uint64_t base_zone_zslba, base_zone_offset, base_zone_num_blocks;

	if (!success) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	while (io_ctx->remain_blocks) {
		// TODO: use '&' operator rather than '%'

		lzslba = io_ctx->next_offset_blocks % congctrl_ns->congctrl_ns_bdev.zone_size;
		zone_array_id = lzslba / congctrl_ns->congctrl_ns_bdev.zone_size;

		lz_offset_blocks = io_ctx->next_offset_blocks - lzslba;
		lz_stripe_idx = lz_offset_blocks / congctrl_ns->stripe_blocks;

		// this base_zone_zslba currently works for the case when ZCAP != ZSZE on the base ZNS bdev
		// base zone ZSLBA of logical io offset
		base_zone_zslba = congctrl_ns->zone_map[zone_array_id] +
						 (lz_stripe_idx % congctrl_ns->zone_array_size) * congctrl_ns->congctrl_ns_bdev.zone_size;
		base_zone_offset = (lz_stripe_idx / congctrl_ns->zone_array_size) * congctrl_ns->stripe_blocks; // base zone internal offset

		io_offset_blocks = base_zone_zslba + base_zone_offset;
		remain_stripe_blocks = congctrl_ns->stripe_blocks - (io_offset_blocks % congctrl_ns->stripe_blocks);
		base_zone_num_blocks = io_ctx->remain_blocks;
		base_zone_num_blocks = base_zone_num_blocks > remain_stripe_blocks ?
								remain_stripe_blocks : base_zone_num_blocks;

		// TODO: check if read io exceeds the zone capacity boundary (ZCAP)
		// If so, those LBAs should behave like deallocated LBAs. (here, return zeroes SPDK_NVME_DEALLOC_READ_00)
		// thus, (io_offset_blocks + base_zone_num_blocks) should not exceed base bdev's ZCAP.

		/* TODO: check if write io exceeds the zone size (ZSZE)
		 * If so, we do followings.
		 * 1. When no current open zone, just open new logical zone
		 * 2. If there is open zone and its write ptr is not at the end, return error
		 * Note: write ptr updates only if the I/O completes. 
		*/

		// TODO: check if this namespace blocklen is not equal to the base blocklen.
		// congctrl_ns_bdev.phys_blocklen != congctrl_ns_bdev.blocklen
		// If so, convert it here

		// TODO: In any case, copy u.bdev.iovs and manipulate for this I/O.
		printf("next_offset_blocks: %llu remain_blocks: %llu\n", io_ctx->next_offset_blocks, io_ctx->remain_blocks);
		printf("zone_size: %llu stripe_blocks: %llu zone_array_size: %llu\n", congctrl_ns->congctrl_ns_bdev.zone_size, congctrl_ns->stripe_blocks, congctrl_ns->zone_array_size);
		printf("lzslba: %llu zone_array_id: %llu lz_offset_blocks: %llu lz_stripe_idx: %llu base_zone_zslba: %llu remain_stripe_blocks: %llu\n", lzslba, zone_array_id, lz_offset_blocks, lz_stripe_idx, base_zone_zslba, remain_stripe_blocks);
		printf("offset: %lu blocks: %lu org_offset:%lu org_blocks:%lu\n", io_offset_blocks, base_zone_num_blocks, bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);

		assert(base_zone_num_blocks>0);

		switch (bdev_io->type) {
		case SPDK_BDEV_IO_TYPE_READ:
			rc = spdk_bdev_readv_blocks(congctrl_node->base_desc, congctrl_ch->base_ch, bdev_io->u.bdev.iovs,
							bdev_io->u.bdev.iovcnt, io_offset_blocks,
							base_zone_num_blocks, _congctrl_complete_io,
							bdev_io);
			break;
		case SPDK_BDEV_IO_TYPE_WRITE:
			rc = spdk_bdev_writev_blocks(congctrl_node->base_desc, congctrl_ch->base_ch, bdev_io->u.bdev.iovs,
							bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.offset_blocks,
							bdev_io->u.bdev.num_blocks, _congctrl_complete_io,
							bdev_io);
			break;
		case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
			rc = spdk_bdev_write_zeroes_blocks(congctrl_node->base_desc, congctrl_ch->base_ch,
							bdev_io->u.bdev.offset_blocks,
							bdev_io->u.bdev.num_blocks,
							_congctrl_complete_io, bdev_io);
			break;
		case SPDK_BDEV_IO_TYPE_ZONE_APPEND:
			rc = spdk_bdev_zone_appendv(congctrl_node->base_desc, congctrl_ch->base_ch, bdev_io->u.bdev.iovs,
							bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.offset_blocks,
							bdev_io->u.bdev.num_blocks, _congctrl_complete_io,
							bdev_io);
			break;
		default:
			rc = -EINVAL;
			SPDK_ERRLOG("congctrl: unknown I/O type %d\n", bdev_io->type);
			break;

		}

		if (rc == -ENOMEM) {
			SPDK_ERRLOG("No memory, start to queue io for delay.\n");
			if (vbdev_congctrl_queue_io(bdev_io) != 0) {
				goto error_out;
			}
		} else if (rc != 0) {
			SPDK_ERRLOG("ERROR on bdev_io submission!\n");
			goto error_out;
		} else {
			// TODO: trace each I/O latency separately
			// define child io array in io_ctx -> set latency tsc for each child io -> pass it as cb_arg for _congctrl_complete_io
			//io_ctx->submit_tick = spdk_get_ticks();
			io_ctx->outstanding_split_ios++;
			io_ctx->next_offset_blocks += base_zone_num_blocks;
			assert(io_ctx->remain_blocks >= base_zone_num_blocks);
			io_ctx->remain_blocks -= base_zone_num_blocks;
		}
	}

	return;

error_out:
	io_ctx->remain_blocks = 0;
	io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
	if (io_ctx->outstanding_split_ios == 0) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
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
			     _congctrl_complete_mgmt, bdev_io);

	if (rc == -ENOMEM) {
		SPDK_ERRLOG("No memory, start to queue io for delay.\n");
		vbdev_congctrl_queue_io(bdev_io);
	} else if (rc != 0) {
		SPDK_ERRLOG("ERROR on bdev_io submission!\n");
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	} else {
		io_ctx->outstanding_split_ios++;
	}
}

static void
_abort_all_congctrled_io(void *arg)
{
	TAILQ_HEAD(, congctrl_bdev_io) *head = arg;
	struct congctrl_bdev_io *io_ctx, *tmp;

	TAILQ_FOREACH_SAFE(io_ctx, head, link, tmp) {
		TAILQ_REMOVE(head, io_ctx, link);
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(io_ctx), SPDK_BDEV_IO_STATUS_ABORTED);
	}
}

static void
vbdev_congctrl_reset_channel(struct spdk_io_channel_iter *i)
{
	struct spdk_io_channel *ch = spdk_io_channel_iter_get_channel(i);
	struct congctrl_io_channel *congctrl_ch = spdk_io_channel_get_ctx(ch);

	_abort_all_congctrled_io(&congctrl_ch->slidewin_wait_queue);
	_abort_all_congctrled_io(&congctrl_ch->write_drr_queue);

	spdk_for_each_channel_continue(i, 0);
}

static int
abort_congctrled_io(void *_head, struct spdk_bdev_io *bio_to_abort)
{
	TAILQ_HEAD(, congctrl_bdev_io) *head = _head;
	struct congctrl_bdev_io *io_ctx_to_abort = (struct congctrl_bdev_io *)bio_to_abort->driver_ctx;
	struct congctrl_bdev_io *io_ctx;

	TAILQ_FOREACH(io_ctx, head, link) {
		if (io_ctx == io_ctx_to_abort) {
			TAILQ_REMOVE(head, io_ctx, link);
			if (io_ctx->outstanding_split_ios == 0 &&
				 io_ctx->remain_blocks == bio_to_abort->u.bdev.num_blocks) {
				// We can abort this I/O that has not yet submited any child commands
				spdk_bdev_io_complete(bio_to_abort, SPDK_BDEV_IO_STATUS_ABORTED);
				return 0;
			} else {
				// We cannot abort I/O in-processing
				return -EBUSY;
			}
		}
	}
	return -ENOENT;
}

static int
vbdev_congctrl_abort(struct vbdev_congctrl *congctrl_node, struct congctrl_io_channel *congctrl_ch,
		  struct spdk_bdev_io *bdev_io)
{
	struct spdk_bdev_io *bio_to_abort = bdev_io->u.abort.bio_to_abort;
	struct congctrl_bdev_io *io_ctx_to_abort = (struct congctrl_bdev_io *)bio_to_abort->driver_ctx;

	if (abort_congctrled_io(&congctrl_ch->slidewin_wait_queue, bio_to_abort) == 0 || 
			abort_congctrled_io(&congctrl_ch->write_drr_queue, bio_to_abort) == 0) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
		return 0;
	} else if (io_ctx_to_abort->type == CONGCTRL_IO_MGMT) {
		return spdk_bdev_abort(congctrl_node->base_desc, congctrl_ch->base_ch, bio_to_abort,
					_congctrl_complete_mgmt, bdev_io);
	} else {
		return -ENOENT;
	}
}

/* We currently don't support a normal I/O command in congctrl_mgmt bdev.
 *  congctrl_mgmt is only used for internal management and creating virtual namespace.
 */
static void
vbdev_congctrl_mgmt_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	SPDK_ERRLOG("congctrl: mgmt does not support a normal I/O type %d\n", bdev_io->type);
	spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	return;
}

static int
_congctrl_ns_mgmt_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct vbdev_congctrl_ns *congctrl_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_congctrl_ns, congctrl_ns_bdev);
	struct vbdev_congctrl	 *congctrl_node = congctrl_ns->ctrl;
	struct congctrl_io_channel *congctrl_ch = spdk_io_channel_get_ctx(ch);
	struct congctrl_bdev_io *io_ctx = (struct congctrl_bdev_io *)bdev_io->driver_ctx;
	int rc = 0;

	io_ctx->type = CONGCTRL_IO_MGMT;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_GET_ZONE_INFO:
		rc = spdk_bdev_get_zone_info(congctrl_node->base_desc, congctrl_ch->base_ch,
						bdev_io->u.zone_mgmt.zone_id, bdev_io->u.zone_mgmt.num_zones,
						bdev_io->u.zone_mgmt.buf, _congctrl_complete_mgmt, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT:
		rc = spdk_bdev_zone_management(congctrl_node->base_desc, congctrl_ch->base_ch,
						bdev_io->u.zone_mgmt.zone_id, bdev_io->u.zone_mgmt.zone_action,
						_congctrl_complete_mgmt, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_UNMAP:
		rc = spdk_bdev_unmap_blocks(congctrl_node->base_desc, congctrl_ch->base_ch,
					    bdev_io->u.bdev.offset_blocks,
					    bdev_io->u.bdev.num_blocks,
					    _congctrl_complete_mgmt, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_FLUSH:
		rc = spdk_bdev_flush_blocks(congctrl_node->base_desc, congctrl_ch->base_ch,
					    bdev_io->u.bdev.offset_blocks,
					    bdev_io->u.bdev.num_blocks,
					    _congctrl_complete_mgmt, bdev_io);
		break;
	default:
		rc = -EINVAL;
		SPDK_ERRLOG("congctrl: unknown I/O type %d\n", bdev_io->type);
		break;
	}
	printf("_congctrl_ns_mgmt_submit_request: %d\n", rc);
	if (rc == 0) {
		io_ctx->outstanding_split_ios++;
	}
	return rc;
}

static void
vbdev_congctrl_ns_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct vbdev_congctrl_ns *congctrl_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_congctrl_ns, congctrl_ns_bdev);
	struct vbdev_congctrl	 *congctrl_node = congctrl_ns->ctrl;
	struct congctrl_io_channel *congctrl_ch = spdk_io_channel_get_ctx(ch);
	struct congctrl_bdev_io *io_ctx = (struct congctrl_bdev_io *)bdev_io->driver_ctx;
	int rc = 0;

	io_ctx->ch = ch;
	io_ctx->outstanding_split_ios = 0;
	io_ctx->status = SPDK_BDEV_IO_STATUS_PENDING;
		
	switch (bdev_io->type) {
	/*
	case SPDK_BDEV_IO_TYPE_READ:
		io_ctx->type = CONGCTRL_IO_READ;
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
		io_ctx->type = CONGCTRL_IO_WRITE;
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
	*/

	// Try to abort I/O if it is a R/W I/O in congestion queues or management command.
	// We cannot abort R/W I/Os already in progress because we may split them.
	case SPDK_BDEV_IO_TYPE_ABORT:
		rc = vbdev_congctrl_abort(congctrl_node, congctrl_ch, bdev_io);
		break;

	// TODO: We may need a special handling for ZONE_RESET in the high capacity utilization because it may impact others.
	// TODO: We need a special handling for ZONE_OPEN/CLOSE for striped zones.
	//case SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT:

	// TODO: We may flush I/O schueduler queues for a congctrl_ns when FLUSH is submitted.
	//       FLUSH may be suspended until all I/O commands in queues complete to emulate FLUSH operation.
	//case SPDK_BDEV_IO_TYPE_FLUSH:

	case SPDK_BDEV_IO_TYPE_RESET:
		// For SPDK_BDEV_IO_TYPE_RESET, we shall abort all I/Os in the scheduling queue before
		// processing delayed I/Os in underlying layers.
		_abort_all_congctrled_io(&congctrl_ch->slidewin_wait_queue);
		_abort_all_congctrled_io(&congctrl_ch->write_drr_queue);
		/* During reset, the generic bdev layer aborts all new I/Os and queues all new resets.
		 * Hence we can simply abort all I/Os delayed to complete.
		 */
		spdk_for_each_channel(congctrl_node, vbdev_congctrl_reset_channel, bdev_io,
				      vbdev_congctrl_reset_dev);
		break;

	// READ
	// TODO : Sliding window scheduling
	case SPDK_BDEV_IO_TYPE_READ:
		io_ctx->type = CONGCTRL_IO_READ;
		io_ctx->remain_blocks = bdev_io->u.bdev.num_blocks;
		io_ctx->next_offset_blocks = bdev_io->u.bdev.offset_blocks;
		spdk_bdev_io_get_buf(bdev_io, _congctrl_ns_rw_cb,
					bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		break;

	// WRITE
	// TODO : DRR scheduling
	case SPDK_BDEV_IO_TYPE_WRITE:
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
	case SPDK_BDEV_IO_TYPE_ZONE_APPEND:
		io_ctx->type = CONGCTRL_IO_WRITE;
		io_ctx->remain_blocks = bdev_io->u.bdev.num_blocks;
		io_ctx->next_offset_blocks = bdev_io->u.bdev.offset_blocks;
		_congctrl_ns_rw_cb(ch, bdev_io, true);
		break;

	default:
		rc = _congctrl_ns_mgmt_submit_request(ch, bdev_io);
		break;;
	}

	if (rc == -EAGAIN) {
		//vbdev_congctrl_cong_io_wait(bdev_io);
		//bdev_io->internal.error.aio_result = rc;
		//spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_AIO_ERROR);
	} else if (rc == -ENOMEM) {
		SPDK_ERRLOG("No memory, start to queue io for congctrl.\n");
		vbdev_congctrl_queue_io(bdev_io);
	} else if (rc != 0) {
		SPDK_ERRLOG("ERROR on bdev_io submission!\n");
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static bool
vbdev_congctrl_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	struct vbdev_congctrl *congctrl_node = (struct vbdev_congctrl *)ctx;

	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_ZCOPY:
	case SPDK_BDEV_IO_TYPE_NVME_ADMIN:
	case SPDK_BDEV_IO_TYPE_NVME_IO:
	case SPDK_BDEV_IO_TYPE_NVME_IO_MD:
	case SPDK_BDEV_IO_TYPE_COMPARE:
	case SPDK_BDEV_IO_TYPE_COMPARE_AND_WRITE:
		return false;
	default:
		return spdk_bdev_io_type_supported(congctrl_node->base_bdev, io_type);
	}
}

static bool
vbdev_congctrl_ns_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	struct vbdev_congctrl_ns *congctrl_ns = (struct vbdev_congctrl_ns *)ctx;

	return vbdev_congctrl_io_type_supported(congctrl_ns->ctrl, io_type);
}

static struct spdk_io_channel *
vbdev_congctrl_get_io_channel(void *ctx)
{
	struct vbdev_congctrl *congctrl_node = (struct vbdev_congctrl *)ctx;

	return spdk_get_io_channel(congctrl_node);
}

static struct spdk_io_channel *
vbdev_congctrl_ns_get_io_channel(void *ctx)
{
	struct vbdev_congctrl *congctrl_ns = (struct vbdev_congctrl *)ctx;

	return spdk_get_io_channel(congctrl_ns);
}

static int
vbdev_congctrl_ns_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct vbdev_congctrl_ns *congctrl_ns = (struct vbdev_congctrl_ns *)ctx;
	/*
	spdk_json_write_name(w, "congctrl_ns");
	spdk_json_write_object_begin(w);

	spdk_json_write_named_string(w, "ns_name", spdk_bdev_get_name(&congctrl_ns->congctrl_ns_bdev));
	spdk_json_write_named_uint64(w, "stripe_size", );

	spdk_json_write_object_end(w);
	*/
	return 0;
}

static void
_congctrl_ns_write_conf_values(struct vbdev_congctrl_ns *congctrl_ns, struct spdk_json_write_ctx *w)
{
	struct vbdev_congctrl *congctrl_node = congctrl_ns->ctrl;

	spdk_json_write_named_string(w, "ns_name", spdk_bdev_get_name(&congctrl_ns->congctrl_ns_bdev));
	spdk_json_write_named_string(w, "ctrl_name", spdk_bdev_get_name(&congctrl_node->mgmt_bdev));
	spdk_json_write_named_uint32(w, "zone_array_size", congctrl_ns->zone_array_size);
	spdk_json_write_named_uint32(w, "stripe_size", congctrl_ns->stripe_blocks);
	spdk_json_write_named_uint32(w, "block_align", congctrl_ns->block_align);
}

static void
_congctrl_write_conf_values(struct vbdev_congctrl *congctrl_node, struct spdk_json_write_ctx *w)
{
	spdk_json_write_named_string(w, "name", spdk_bdev_get_name(&congctrl_node->mgmt_bdev));
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
	struct vbdev_congctrl_ns *congctrl_ns;

	spdk_json_write_name(w, "congctrl");
	spdk_json_write_object_begin(w);
	_congctrl_write_conf_values(congctrl_node, w);

	spdk_json_write_named_array_begin(w, "namespaces");
	TAILQ_FOREACH(congctrl_ns, &congctrl_node->ns, link) {
		spdk_json_write_string(w, spdk_bdev_get_name(&congctrl_ns->congctrl_ns_bdev));
	}

	spdk_json_write_array_end(w);
	spdk_json_write_object_end(w);

	return 0;
}

/* This is used to generate JSON that can configure this module to its current state. */
static int
vbdev_congctrl_config_json(struct spdk_json_write_ctx *w)
{
	struct vbdev_congctrl *congctrl_node;
	struct vbdev_congctrl_ns *congctrl_ns;

	TAILQ_FOREACH(congctrl_node, &g_congctrl_nodes, link) {
		spdk_json_write_object_begin(w);
		spdk_json_write_named_string(w, "method", "bdev_congctrl_create");
		spdk_json_write_named_object_begin(w, "params");
		_congctrl_write_conf_values(congctrl_node, w);
		spdk_json_write_object_end(w);
		spdk_json_write_object_end(w);

		TAILQ_FOREACH(congctrl_ns, &congctrl_node->ns, link) {
			spdk_json_write_object_begin(w);
			spdk_json_write_named_string(w, "method", "bdev_congctrl_ns_create");
			spdk_json_write_named_object_begin(w, "params");
			_congctrl_ns_write_conf_values(congctrl_ns, w);
			spdk_json_write_object_end(w);
			spdk_json_write_object_end(w);
		}
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
congctrl_bdev_io_ch_create_cb(void *io_device, void *ctx_buf)
{
	struct congctrl_io_channel *congctrl_ch = ctx_buf;
	struct vbdev_congctrl_ns *congctrl_ns = io_device;
	struct vbdev_congctrl *congctrl_node = congctrl_ns->ctrl;

	congctrl_ch->base_ch = spdk_bdev_get_io_channel(congctrl_node->base_desc);

	return 0;
}

static int
congctrl_bdev_mgmt_ch_create_cb(void *io_device, void *ctx_buf)
{
	struct congctrl_mgmt_channel *congctrl_mgmt_ch = ctx_buf;
	struct vbdev_congctrl *congctrl_node = io_device;

	congctrl_mgmt_ch->io_poller = SPDK_POLLER_REGISTER(_congctrl_cong_update, congctrl_mgmt_ch, 100);
	congctrl_mgmt_ch->top_poller = SPDK_POLLER_REGISTER(_congctrl_cong_top, congctrl_mgmt_ch, 1000000);
	congctrl_mgmt_ch->base_ch = spdk_bdev_get_io_channel(congctrl_node->base_desc);

	return 0;
}

/* We provide this callback for the SPDK channel code to destroy a channel
 * created with our create callback. We just need to undo anything we did
 * when we created. If this bdev used its own poller, we'd unregsiter it here.
 */
static void
congctrl_bdev_io_ch_destroy_cb(void *io_device, void *ctx_buf)
{
	struct congctrl_io_channel *congctrl_ch = ctx_buf;

	spdk_put_io_channel(congctrl_ch->base_ch);
}

static void
congctrl_bdev_mgmt_ch_destroy_cb(void *io_device, void *ctx_buf)
{
	struct congctrl_mgmt_channel *congctrl_mgmt_ch = ctx_buf;

	spdk_poller_unregister(&congctrl_mgmt_ch->top_poller);
	spdk_poller_unregister(&congctrl_mgmt_ch->io_poller);
	spdk_put_io_channel(congctrl_mgmt_ch->base_ch);
}

/* Create the congctrl association from the bdev and vbdev name and insert
 * on the global list. */
static int
vbdev_congctrl_ns_insert_association(const char *congctrl_name, const char *ns_name,
					uint32_t zone_array_size, uint32_t stripe_size, uint32_t block_align)
{
	struct bdev_association *bdev_assoc;
	struct ns_association *assoc;

	TAILQ_FOREACH(bdev_assoc, &g_bdev_associations, link) {
		if (strcmp(congctrl_name, bdev_assoc->vbdev_name)) {
			continue;
		}

		TAILQ_FOREACH(assoc, &g_ns_associations, link) {
			if (strcmp(ns_name, assoc->ns_name) == 0 && strcmp(congctrl_name, assoc->ctrl_name) == 0) {
				SPDK_ERRLOG("congctrl ns bdev %s/%s already exists\n", congctrl_name, ns_name);
				return -EEXIST;
			}
		}

		assoc = calloc(1, sizeof(struct ns_association));
		if (!assoc) {
			SPDK_ERRLOG("could not allocate bdev_association\n");
			return -ENOMEM;
		}

		assoc->ctrl_name = strdup(congctrl_name);
		if (!assoc->ctrl_name) {
			SPDK_ERRLOG("could not allocate assoc->ctrl_name\n");
			free(assoc);
			return -ENOMEM;
		}

		assoc->ns_name = strdup(ns_name);
		if (!assoc->ns_name) {
			SPDK_ERRLOG("could not allocate assoc->ns_name\n");
			free(assoc->ctrl_name);
			free(assoc);
			return -ENOMEM;
		}

		// TODO: assign parameter values
		assoc->zone_array_size = zone_array_size;
		assoc->stripe_size = stripe_size;
		assoc->block_align = block_align;
		TAILQ_INSERT_TAIL(&g_ns_associations, assoc, link);

		return 0;
	}

	SPDK_ERRLOG("Unable to insert ns %s assoc because the congctrl bdev %s doesn't exist.\n", ns_name, congctrl_name);
	return -ENODEV;
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
		  uint64_t latency_lower, enum congctrl_io_type type)
{
	struct bdev_association *assoc;
	struct spdk_bdev *mgmt_bdev;
	struct vbdev_congctrl *congctrl_node;
	uint64_t ticks_mhz = spdk_get_ticks_hz() / SPDK_SEC_TO_USEC;
	int rc = -EINVAL;

	if (type >= CONGCTRL_IO_MGMT) {
		return rc;
	}
 
	TAILQ_FOREACH(assoc, &g_bdev_associations, link) {
		if (strcmp(assoc->vbdev_name, congctrl_name) != 0) {
			continue;
		}

		rc = 0;
		switch (type) {
		case CONGCTRL_IO_READ:
			assoc->upper_read_latency = ticks_mhz * latency_upper;
			assoc->lower_read_latency = ticks_mhz * latency_lower;
			break;
		case CONGCTRL_IO_WRITE:
			assoc->upper_write_latency = ticks_mhz * latency_upper;
			assoc->lower_write_latency = ticks_mhz * latency_lower;
			break;
		default:
			break;
		}

		mgmt_bdev = spdk_bdev_get_by_name(congctrl_name);
		if (mgmt_bdev && mgmt_bdev->module == &congctrl_if) {
			congctrl_node = SPDK_CONTAINEROF(mgmt_bdev, struct vbdev_congctrl, mgmt_bdev);

			congctrl_node->upper_read_latency = assoc->upper_read_latency;
			congctrl_node->lower_read_latency = assoc->lower_read_latency;
			congctrl_node->upper_write_latency = assoc->upper_write_latency;
			congctrl_node->lower_write_latency = assoc->lower_write_latency;
		}
	}

	return rc;
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
	struct bdev_association *bdev_assoc;
	struct ns_association *ns_assoc;

	while ((bdev_assoc = TAILQ_FIRST(&g_bdev_associations))) {
		TAILQ_REMOVE(&g_bdev_associations, bdev_assoc, link);
		free(bdev_assoc->bdev_name);
		free(bdev_assoc->vbdev_name);
		free(bdev_assoc);
	}
	while ((ns_assoc = TAILQ_FIRST(&g_ns_associations))) {
		TAILQ_REMOVE(&g_ns_associations, ns_assoc, link);
		free(ns_assoc->ctrl_name);
		free(ns_assoc->ns_name);
		free(ns_assoc);
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

	/* congctrl bdev doesn't work with data buffers, so it supports any memory domain used by base_bdev */
	return spdk_bdev_get_memory_domains(congctrl_node->base_bdev, domains, array_size);
}

/* When we register our bdev this is how we specify our entry points. */
static const struct spdk_bdev_fn_table vbdev_congctrl_ns_fn_table = {
	.destruct		= vbdev_congctrl_ns_destruct,
	.submit_request		= vbdev_congctrl_ns_submit_request,
	.io_type_supported	= vbdev_congctrl_ns_io_type_supported,
	.get_io_channel		= vbdev_congctrl_ns_get_io_channel,
	.dump_info_json		= vbdev_congctrl_ns_dump_info_json,
	.write_config_json	= NULL,
	.get_memory_domains	= vbdev_congctrl_get_memory_domains,
};

static int
vbdev_congctrl_ns_register(const char *ctrl_name, const char *ns_name)
{
	struct ns_association *assoc;
	struct vbdev_congctrl *congctrl_node;
	struct vbdev_congctrl_ns *congctrl_ns = NULL;
	struct spdk_bdev *bdev;
	int rc = 0;

	TAILQ_FOREACH(assoc, &g_ns_associations, link) {
		if (strcmp(assoc->ctrl_name, ctrl_name)) {
			continue;
		} else if (ns_name && strcmp(assoc->ns_name, ns_name)) {
			continue;
		}

		TAILQ_FOREACH(congctrl_node, &g_congctrl_nodes, link) {
			if (strcmp(congctrl_node->mgmt_bdev.name, ctrl_name)) {
				continue;
			}

			TAILQ_FOREACH(congctrl_ns, &congctrl_node->ns, link) {
				if (!strcmp(congctrl_ns->congctrl_ns_bdev.name, assoc->ns_name)) {
					SPDK_ERRLOG("the ns name %s already exists on congctrl_node %s\n", assoc->ns_name, ctrl_name);
					return -EEXIST;
				}
			}

			congctrl_ns = calloc(1, sizeof(struct vbdev_congctrl_ns) + sizeof(uint64_t) * 102400);
			if (!congctrl_ns) {
				rc = -ENOMEM;
				SPDK_ERRLOG("could not allocate congctrl_node\n");
				break;
			}
			congctrl_ns->congctrl_ns_bdev.name = strdup(assoc->ns_name);
			if (!congctrl_ns->congctrl_ns_bdev.name) {
				rc = -ENOMEM;
				SPDK_ERRLOG("could not allocate congctrl_bdev name\n");
				free(congctrl_ns);
				break;
			}
			congctrl_ns->congctrl_ns_bdev.product_name = "congctrl";

			congctrl_ns->ctrl = congctrl_node;
			bdev = congctrl_node->base_bdev;

			congctrl_ns->congctrl_ns_bdev.ctxt = congctrl_ns;
			congctrl_ns->congctrl_ns_bdev.fn_table = &vbdev_congctrl_ns_fn_table;
			congctrl_ns->congctrl_ns_bdev.module = &congctrl_if;

			congctrl_ns->congctrl_ns_bdev.zoned = true;

			// Configure namespace specific parameters
			congctrl_ns->zone_array_size = assoc->zone_array_size ? assoc->zone_array_size : 1;
			for (uint64_t i=0; i < 102400; i++) {
				congctrl_ns->zone_map[i] = congctrl_node->mgmt_bdev.zone_size * i * congctrl_ns->zone_array_size;
			}

			if (assoc->stripe_size && (assoc->stripe_size % bdev->blocklen)) { 
				rc = -EINVAL;
				SPDK_ERRLOG("stripe size must be block size aligned\n");
				free(congctrl_ns);
				break;
			}
			congctrl_ns->stripe_blocks = assoc->stripe_size ? (assoc->stripe_size / bdev->blocklen) -1 : bdev->optimal_io_boundary;
			if (congctrl_ns->stripe_blocks == 0) {
				congctrl_ns->stripe_blocks = 1;
			}
			if (bdev->zone_size % congctrl_ns->stripe_blocks) {
				rc = -EINVAL;
				SPDK_ERRLOG("base bdev zone size must be stripe size aligned\n");
				free(congctrl_ns);
				break;
			}
			congctrl_ns->block_align = assoc->block_align;

			congctrl_ns->congctrl_ns_bdev.write_cache = bdev->write_cache;
			congctrl_ns->congctrl_ns_bdev.optimal_io_boundary = bdev->optimal_io_boundary;

			congctrl_ns->zcap = bdev->zone_size * congctrl_ns->zone_array_size;
			//congctrl_ns->congctrl_ns_bdev.zone_size = spdk_align64pow2(congctrl_ns->zcap);
			congctrl_ns->congctrl_ns_bdev.zone_size = congctrl_ns->zcap;
			congctrl_ns->congctrl_ns_bdev.required_alignment = bdev->required_alignment;
			congctrl_ns->congctrl_ns_bdev.max_zone_append_size = bdev->max_zone_append_size;
			congctrl_ns->congctrl_ns_bdev.max_open_zones = 1;
			congctrl_ns->congctrl_ns_bdev.max_active_zones = 1;
			congctrl_ns->congctrl_ns_bdev.optimal_open_zones = 1;
		
			congctrl_ns->congctrl_ns_bdev.blocklen = bdev->blocklen;
			// TODO: support configurable block length (blocklen)
			//congctrl_ns->congctrl_ns_bdev.phys_blocklen = bdev->blocklen;
			// TODO: support logical bdev size (blockcnt)
			congctrl_ns->congctrl_ns_bdev.blockcnt = bdev->blockcnt;


			spdk_io_device_register(congctrl_ns, congctrl_bdev_io_ch_create_cb, congctrl_bdev_io_ch_destroy_cb,
						sizeof(struct congctrl_io_channel),
						assoc->ns_name);
			
			rc = spdk_bdev_register(&congctrl_ns->congctrl_ns_bdev);
			if (rc) {
				SPDK_ERRLOG("could not register congctrl_ns_bdev\n");
				goto error_close;
			}

			TAILQ_INSERT_TAIL(&congctrl_node->ns, congctrl_ns, link);
		}
	}

	return 0;

error_close:
	spdk_io_device_unregister(congctrl_ns, NULL);
	free(congctrl_ns->congctrl_ns_bdev.name);
	free(congctrl_ns);
	return rc;
}

static void
vbdev_congctrl_base_bdev_hotremove_cb(struct spdk_bdev *bdev_find)
{
	struct vbdev_congctrl *congctrl_node, *tmp;

	TAILQ_FOREACH_SAFE(congctrl_node, &g_congctrl_nodes, link, tmp) {
		if (bdev_find == congctrl_node->base_bdev) {
			spdk_bdev_unregister(&congctrl_node->mgmt_bdev, NULL, NULL);
		}
	}
}

/* When we register our bdev this is how we specify our entry points. */
static const struct spdk_bdev_fn_table vbdev_congctrl_fn_table = {
	.destruct		= vbdev_congctrl_destruct,
	.submit_request		= vbdev_congctrl_mgmt_submit_request,
	.io_type_supported	= vbdev_congctrl_io_type_supported,
	.get_io_channel		= vbdev_congctrl_get_io_channel,
	.dump_info_json		= vbdev_congctrl_dump_info_json,
	.write_config_json	= NULL,
	.get_memory_domains	= vbdev_congctrl_get_memory_domains,
};

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
		TAILQ_INIT(&congctrl_node->ns);

		// Create the congctrl mgmt_ns
		congctrl_node->mgmt_bdev.name = strdup(assoc->vbdev_name);
		if (!congctrl_node->mgmt_bdev.name) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate congctrl_bdev name\n");
			free(congctrl_node);
			break;
		}
		congctrl_node->mgmt_bdev.product_name = "congctrl";

		/* The base bdev that we're attaching to. */
		rc = spdk_bdev_open_ext(bdev_name, true, vbdev_congctrl_base_bdev_event_cb,
					NULL, &congctrl_node->base_desc);

		if (rc) {
			if (rc != -ENODEV) {
				SPDK_ERRLOG("could not open bdev %s\n", bdev_name);
			}
			free(congctrl_node->mgmt_bdev.name);
			free(congctrl_node);
			break;
		}

		bdev = spdk_bdev_desc_get_bdev(congctrl_node->base_desc);
		if (!spdk_bdev_is_zoned(bdev)) {
			rc = -EINVAL;
			SPDK_ERRLOG("congctrl does not support non-zoned devices\n");
			free(congctrl_node->mgmt_bdev.name);
			free(congctrl_node);
			break;
		}
		congctrl_node->base_bdev = bdev;

		congctrl_node->mgmt_bdev.write_cache = bdev->write_cache;
		congctrl_node->mgmt_bdev.required_alignment = bdev->required_alignment;
		congctrl_node->mgmt_bdev.optimal_io_boundary = bdev->optimal_io_boundary;
		congctrl_node->mgmt_bdev.blocklen = bdev->blocklen;
		congctrl_node->mgmt_bdev.blockcnt = bdev->blockcnt;

		congctrl_node->mgmt_bdev.zoned = true;
		congctrl_node->mgmt_bdev.zone_size = bdev->zone_size;
		congctrl_node->mgmt_bdev.max_zone_append_size = bdev->max_zone_append_size;
		congctrl_node->mgmt_bdev.max_open_zones = bdev->max_open_zones;
		congctrl_node->mgmt_bdev.max_active_zones = bdev->max_active_zones;
		congctrl_node->mgmt_bdev.optimal_open_zones = bdev->optimal_open_zones;

		congctrl_node->mgmt_bdev.ctxt = congctrl_node;
		congctrl_node->mgmt_bdev.fn_table = &vbdev_congctrl_fn_table;
		congctrl_node->mgmt_bdev.module = &congctrl_if;

		/* Store the number of ticks you need to add to get the I/O expiration time. */
		congctrl_node->upper_read_latency = ticks_mhz * assoc->upper_read_latency;
		congctrl_node->lower_read_latency = ticks_mhz * assoc->lower_read_latency;
		congctrl_node->upper_write_latency = ticks_mhz * assoc->upper_write_latency;
		congctrl_node->lower_write_latency = ticks_mhz * assoc->lower_write_latency;

		/* set 0 to current claimed blockcnt */
		congctrl_node->claimed_blockcnt = 0;

		spdk_io_device_register(congctrl_node, congctrl_bdev_mgmt_ch_create_cb, congctrl_bdev_mgmt_ch_destroy_cb,
					sizeof(struct congctrl_io_channel),
					assoc->vbdev_name);

		/* Save the thread where the base device is opened */
		congctrl_node->thread = spdk_get_thread();

		/* claim the base_bdev only if this is the first congctrl node */
		rc = spdk_bdev_module_claim_bdev(bdev, congctrl_node->base_desc, congctrl_node->mgmt_bdev.module);
		if (rc) {
			SPDK_ERRLOG("could not claim bdev %s\n", bdev_name);
			goto error_close;
		}

		rc = spdk_bdev_register(&congctrl_node->mgmt_bdev);
		if (rc) {
			SPDK_ERRLOG("could not register congctrl mgmt bdev\n");
			spdk_bdev_module_release_bdev(bdev);
			goto error_close;
		}

		rc = vbdev_congctrl_ns_register(congctrl_node->mgmt_bdev.name, NULL);
		if (rc) {
			SPDK_ERRLOG("Unable to create ns on the congctrl bdev %s.\n", congctrl_node->mgmt_bdev.name);
			spdk_bdev_module_release_bdev(bdev);
			goto error_close;
		}

		TAILQ_INSERT_TAIL(&g_congctrl_nodes, congctrl_node, link);
	}

	return rc;

error_close:
	spdk_bdev_close(congctrl_node->base_desc);
	spdk_io_device_unregister(congctrl_node, NULL);
	free(congctrl_node->mgmt_bdev.name);
	free(congctrl_node);
	return rc;
}

int
create_congctrl_ns(const char *congctrl_name, const char *ns_name,
					uint32_t zone_array_size, uint32_t stripe_size, uint32_t block_align)
{
	struct ns_association *assoc;
	int rc = 0;

	rc = vbdev_congctrl_ns_insert_association(congctrl_name, ns_name,
										 zone_array_size, stripe_size, block_align);
	if (rc) {
		return rc;
	}

	rc = vbdev_congctrl_ns_register(congctrl_name, ns_name);
	if (rc) {
		TAILQ_FOREACH(assoc, &g_ns_associations, link) {
			if (strcmp(assoc->ns_name, ns_name) == 0) {
				TAILQ_REMOVE(&g_ns_associations, assoc, link);
				free(assoc->ctrl_name);
				free(assoc->ns_name);
				free(assoc);
				break;
			}
		}
	}

	return rc;
}

void
delete_congctrl_ns(struct spdk_bdev *bdev, spdk_bdev_unregister_cb cb_fn, void *cb_arg)
{
	struct ns_association *assoc;

	if (!bdev || bdev->module != &congctrl_if) {
		cb_fn(cb_arg, -ENODEV);
		return;
	}

	TAILQ_FOREACH(assoc, &g_ns_associations, link) {
		if (strcmp(assoc->ns_name, bdev->name) == 0) {
			SPDK_NOTICELOG("association of vbdev ns %s deleted\n", assoc->ns_name);
			TAILQ_REMOVE(&g_ns_associations, assoc, link);
			free(assoc->ctrl_name);
			free(assoc->ns_name);
			free(assoc);
			break;
		}
	}

	spdk_bdev_unregister(bdev, cb_fn, cb_arg);
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
		goto error_close;
	}

	return rc;

error_close:
	TAILQ_FOREACH(assoc, &g_bdev_associations, link) {
		if (strcmp(assoc->vbdev_name, vbdev_name) == 0) {
			TAILQ_REMOVE(&g_bdev_associations, assoc, link);
			free(assoc->bdev_name);
			free(assoc->vbdev_name);
			free(assoc);
			break;
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

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

#include "spdk/log.h"
#include "spdk/env.h"
#include "spdk/likely.h"

#include "vbdev_detzone.h"
#include "vbdev_detzone_internal.h"

static int vbdev_detzone_init(void);
static int vbdev_detzone_get_ctx_size(void);
static void vbdev_detzone_examine(struct spdk_bdev *bdev);
static void vbdev_detzone_finish(void);
static int vbdev_detzone_config_json(struct spdk_json_write_ctx *w);

static struct spdk_bdev_module detzone_if = {
	.name = "detzone",
	.module_init = vbdev_detzone_init,
	.get_ctx_size = vbdev_detzone_get_ctx_size,
	.examine_config = vbdev_detzone_examine,
	.module_fini = vbdev_detzone_finish,
	.config_json = vbdev_detzone_config_json
};

SPDK_BDEV_MODULE_REGISTER(detzone, &detzone_if)

/* detzone associative list to be used in examine */
struct bdev_association {
	char			*vbdev_name;
	char			*bdev_name;
	uint32_t		num_pu;
	TAILQ_ENTRY(bdev_association)	link;
};
static TAILQ_HEAD(, bdev_association) g_bdev_associations = TAILQ_HEAD_INITIALIZER(
			g_bdev_associations);

/* detzone_ns associative list to be used in examine */
struct ns_association {
	char			*ctrl_name;
	char			*ns_name;

	// Namespace specific parameters
	uint32_t	zone_array_size;
	uint32_t	stripe_size;
	uint32_t	block_align;

	uint64_t	start_base_zone;
	uint64_t	num_base_zones;

	TAILQ_ENTRY(ns_association)	link;
};
static TAILQ_HEAD(, ns_association) g_ns_associations = TAILQ_HEAD_INITIALIZER(
			g_ns_associations);


static TAILQ_HEAD(, vbdev_detzone) g_detzone_ctrlrs = TAILQ_HEAD_INITIALIZER(g_detzone_ctrlrs);

static void vbdev_detzone_ns_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io);
static int _detzone_ns_io_submit(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io);

static int
vbdev_detzone_slidewin_empty(struct detzone_bdev_io *io_ctx)
{
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(io_ctx->ch);

	if (io_ctx->type == DETZONE_IO_READ) {
		return TAILQ_EMPTY(&detzone_ch->rd_slidewin_queue);
	} else {
		return TAILQ_EMPTY(&detzone_ch->wr_slidewin_queue);
	}
}

static void
vbdev_detzone_slidewin_enqueue(struct detzone_bdev_io *io_ctx)
{
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(io_ctx->ch);

	if (io_ctx->type == DETZONE_IO_READ) {
		TAILQ_INSERT_TAIL(&detzone_ch->rd_slidewin_queue, io_ctx, link);
	} else {
		TAILQ_INSERT_TAIL(&detzone_ch->wr_slidewin_queue, io_ctx, link);
	}
}

static void
vbdev_detzone_slidewin_requeue(struct detzone_bdev_io *io_ctx)
{
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(io_ctx->ch);

	if (io_ctx->type == DETZONE_IO_READ) {
		TAILQ_INSERT_HEAD(&detzone_ch->rd_slidewin_queue, io_ctx, link);
	} else {
		TAILQ_INSERT_HEAD(&detzone_ch->wr_slidewin_queue, io_ctx, link);
	}
}

static void *
vbdev_detzone_slidewin_dequeue(struct detzone_io_channel *detzone_ch, enum detzone_io_type type)
{
	struct detzone_bdev_io *io_ctx;

	if (type == DETZONE_IO_READ) {
		io_ctx = TAILQ_FIRST(&detzone_ch->rd_slidewin_queue);
		if (io_ctx) {
			TAILQ_REMOVE(&detzone_ch->rd_slidewin_queue, io_ctx, link);
		}
	} else {
		io_ctx = TAILQ_FIRST(&detzone_ch->wr_slidewin_queue);
		if (io_ctx) {
			TAILQ_REMOVE(&detzone_ch->wr_slidewin_queue, io_ctx, link);
		}
	}
	return io_ctx;	
}

static void
vbdev_detzone_slidewin_submit(struct detzone_bdev_io *io_ctx, uint64_t submit_blks)
{
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(io_ctx->ch);

	if (io_ctx->type == DETZONE_IO_READ) {
		detzone_ch->rd_avail_window -= submit_blks;
	} else {
		detzone_ch->wr_avail_window -= submit_blks;
	}
}

static void
vbdev_detzone_slidewin_complete(struct detzone_bdev_io *io_ctx, uint64_t cpl_blks)
{
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(io_ctx->ch);

	if (io_ctx->type == DETZONE_IO_READ) {
		detzone_ch->rd_avail_window += cpl_blks;
	} else {
		detzone_ch->wr_avail_window += cpl_blks;
	}
}

static uint64_t
vbdev_detzone_slidewin_avail(struct detzone_bdev_io *io_ctx)
{
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(io_ctx->ch);

	if (io_ctx->type == DETZONE_IO_READ) {
		return UINT64_MAX;
		return detzone_ch->rd_avail_window;
	} else {
		return detzone_ch->wr_avail_window;
	}
}

static void
_vbdev_detzone_slidewin_resched_read(void *arg)
{
	struct spdk_io_channel *ch = (struct spdk_io_channel *)arg;
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);
	struct spdk_bdev_io		*bdev_io;
	struct detzone_bdev_io *io_ctx;

	while (detzone_ch->rd_avail_window) {
		io_ctx = vbdev_detzone_slidewin_dequeue(detzone_ch, DETZONE_IO_READ);
		if (io_ctx == NULL) {
			break;
		} else {
			bdev_io = spdk_bdev_io_from_ctx(io_ctx);
			if (_detzone_ns_io_submit(ch, bdev_io) != 0) {
				spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
				break;
			}
		}
	}
}

static void
_vbdev_detzone_slidewin_resched_write(void *arg)
{
	struct spdk_io_channel *ch = (struct spdk_io_channel *)arg;
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);
	struct spdk_bdev_io		*bdev_io;
	struct detzone_bdev_io *io_ctx;

	while (detzone_ch->wr_avail_window) {
		io_ctx = vbdev_detzone_slidewin_dequeue(detzone_ch, DETZONE_IO_WRITE);
		if (io_ctx == NULL) {
			break;
		} else {
			bdev_io = spdk_bdev_io_from_ctx(io_ctx);
			if (_detzone_ns_io_submit(ch, bdev_io) != 0) {
				spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
				break;
			}
		}
	}
}

static void
vbdev_detzone_slidewin_resume(void *arg)
{
	struct spdk_io_channel *ch = (struct spdk_io_channel *)arg;

	_vbdev_detzone_slidewin_resched_read(ch);
	_vbdev_detzone_slidewin_resched_write(ch);
}

static inline uint64_t
_vbdev_detzone_get_lzone_idx(struct vbdev_detzone_ns *detzone_ns, uint64_t lba)
{
	// TODO: use shift operator
	return lba / detzone_ns->detzone_ns_bdev.zone_size;
}

static inline uint64_t
_vbdev_detzone_get_base_offset(struct vbdev_detzone_ns *detzone_ns, uint64_t lba)
{
	//struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	//printf("phys_slba:%lu base_slba_off:%lu stripe_off:%lu lba_off:%lu\n", detzone_ns->start_base_zone_id +
	//		(lba / detzone_ns->detzone_ns_bdev.zone_size) * (detzone_ns->zone_array_size * detzone_ns->base_zone_size),
	//		((((lba % detzone_ns->detzone_ns_bdev.zone_size) / detzone_ns->stripe_blocks)) % detzone_ns->zone_array_size) * detzone_ns->base_zone_size,
	//		((lba % detzone_ns->detzone_ns_bdev.zone_size) / (detzone_ns->zone_array_size * detzone_ns->stripe_blocks)) * detzone_ns->stripe_blocks,
	//		lba % detzone_ns->stripe_blocks);
	// TODO: use shift operator
	uint64_t zone_idx = _vbdev_detzone_get_lzone_idx(detzone_ns, lba);
	uint64_t stripe_idx = (((lba % detzone_ns->detzone_ns_bdev.zone_size) / detzone_ns->stripe_blocks)) % detzone_ns->zone_array_size;
	uint64_t stripe_offset = ((lba % detzone_ns->detzone_ns_bdev.zone_size) / (detzone_ns->zone_array_size * detzone_ns->stripe_blocks)) * detzone_ns->stripe_blocks;

	assert(detzone_ns->zone_info[zone_idx].base_zone_id[stripe_idx] != UINT64_MAX);
	return detzone_ns->zone_info[zone_idx].base_zone_id[stripe_idx] + stripe_offset + (lba % detzone_ns->stripe_blocks);
	/*
	return detzone_ns->start_base_zone_id
			+ (lba / detzone_ns->detzone_ns_bdev.zone_size) * (detzone_ns->zone_array_size * detzone_ns->base_zone_size) // phys slba of lzone
			+ ((((lba % detzone_ns->detzone_ns_bdev.zone_size) / detzone_ns->stripe_blocks)) % detzone_ns->zone_array_size) * detzone_ns->base_zone_size // base slba offset from phy slba of lzone
			+ ((lba % detzone_ns->detzone_ns_bdev.zone_size) / (detzone_ns->zone_array_size * detzone_ns->stripe_blocks)) * detzone_ns->stripe_blocks // stripe offset in base phys zone
			+ lba % detzone_ns->stripe_blocks; // lba offset in stripe
		//detzone_ns->base_zcap
	*/
}

static inline void
_vbdev_detzone_ns_forward_wp(struct vbdev_detzone_ns *detzone_ns, uint64_t offset_blocks, uint64_t numblocks)
{
	uint64_t zone_idx = _vbdev_detzone_get_lzone_idx(detzone_ns, offset_blocks);
	detzone_ns->zone_info[zone_idx].write_pointer += numblocks;
	if (detzone_ns->zone_info[zone_idx].write_pointer == detzone_ns->zone_info[zone_idx].capacity) {
		detzone_ns->zone_info[zone_idx].write_pointer = detzone_ns->detzone_ns_bdev.zone_size;
		detzone_ns->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_FULL;
		detzone_ns->num_open_lzones--;
	}
}

/* Completion callback for IO that were issued from this bdev. The original bdev_io
 * is passed in as an arg so we'll complete that one with the appropriate status
 * and then free the one that this module issued.
 */
static void
_detzone_ns_complete_io(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(orig_io->bdev, struct vbdev_detzone_ns,
					 detzone_ns_bdev);
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)orig_io->driver_ctx;
	uint32_t cdw0 = 0;
	int sct = SPDK_NVME_SCT_GENERIC, sc = SPDK_NVME_SC_SUCCESS;

	spdk_bdev_free_io(bdev_io);
	if (!success) {
		io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
		spdk_bdev_io_get_nvme_status(bdev_io, &cdw0, &sct, &sc);
		spdk_bdev_io_complete_nvme_status(orig_io, cdw0, sct, sc);
	}

	if (io_ctx->type == DETZONE_IO_WRITE && success) {
		// TODO: Check latency and throughput for write I/O on every successful write completion.
		//       If the latency is higher than actual throughput, it implies write cache overflow.
		io_ctx->completion_tick = spdk_get_ticks();
	}

	vbdev_detzone_slidewin_complete(io_ctx, bdev_io->u.bdev.num_blocks);
	if (!vbdev_detzone_slidewin_empty(io_ctx)) {
		spdk_thread_send_msg(spdk_bdev_io_get_thread(bdev_io),
				 (io_ctx->type == DETZONE_IO_READ) ?
				 		 _vbdev_detzone_slidewin_resched_read : _vbdev_detzone_slidewin_resched_write,
				 io_ctx->ch);
	}

	assert(io_ctx->outstanding_stripe_ios);
	io_ctx->outstanding_stripe_ios--;
	//printf("spdk_bdev_io_complete: rc:%d out_ios:%u remain_blocks:%lu\n", io_ctx->status, io_ctx->outstanding_stripe_ios, io_ctx->remain_blocks);
	if (io_ctx->outstanding_stripe_ios == 0 && io_ctx->remain_blocks == 0) {
		if (io_ctx->status != SPDK_BDEV_IO_STATUS_FAILED) {
			io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
		}
		if (io_ctx->type == DETZONE_IO_WRITE) {
			if (io_ctx->status == SPDK_BDEV_IO_STATUS_SUCCESS) {
				_vbdev_detzone_ns_forward_wp(detzone_ns, orig_io->u.bdev.offset_blocks, orig_io->u.bdev.num_blocks);
			} else {
				// TODO: Check phys zone state and write pointer.
				// If the failure happened in the middle of I/O, we should finish the lzone with ZFC bit set to '1'
				// otherwise, we move the write pointer accordingly.
				assert(0);
			}
		}
		spdk_bdev_io_complete(orig_io, io_ctx->status);		
	}
	return;
}

/// Completion callback for mgmt commands that were issued from this bdev.
static void
_detzone_complete_mgmt(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;

	spdk_bdev_free_io(bdev_io);
	spdk_bdev_io_complete(orig_io, success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED);
	return;
}

static void
vbdev_detzone_resubmit_io(void *arg)
{
	struct spdk_bdev_io *bdev_io = (struct spdk_bdev_io *)arg;
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;

	vbdev_detzone_ns_submit_request(io_ctx->ch, bdev_io);
}

static int
vbdev_detzone_queue_io(struct spdk_bdev_io *bdev_io)
{
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(io_ctx->ch);

	io_ctx->bdev_io_wait.bdev = bdev_io->bdev;
	io_ctx->bdev_io_wait.cb_fn = vbdev_detzone_resubmit_io;
	io_ctx->bdev_io_wait.cb_arg = bdev_io;

	return spdk_bdev_queue_io_wait(bdev_io->bdev, detzone_ch->base_ch, &io_ctx->bdev_io_wait);
}

static int
vbdev_detzone_ns_lzone_recv(struct vbdev_detzone_ns *detzone_ns,
								 uint32_t buf_length, void *buf, uint64_t lzslba,
								 enum spdk_nvme_zns_zra_report_opts report_opts,
								 enum spdk_nvme_zns_zone_receive_action zra, bool partial)
{
	struct spdk_nvme_zns_zone_report *zone_report = buf;
	uint64_t max_zones_per_buf = (buf_length - sizeof(*zone_report)) /
			    sizeof(zone_report->descs[0]);
	uint64_t zone_idx, total_zones;
	size_t i;

	if (zra != SPDK_NVME_ZONE_REPORT || report_opts != SPDK_NVME_ZRA_LIST_ALL) {
		//TODO : implement other actions and opts
		return -EINVAL;
	}

	total_zones = detzone_ns->detzone_ns_bdev.blockcnt / detzone_ns->detzone_ns_bdev.zone_size;
	zone_idx = _vbdev_detzone_get_lzone_idx(detzone_ns, lzslba);
	if (zone_idx >= total_zones) {
		return -EINVAL;
	}
	/* User can request info for more zones than exist, need to check both internal and user
	 * boundaries
	 */
	for (i = 0; i < max_zones_per_buf && zone_idx+i < total_zones; i++) {
		zone_report->descs[i].zt = SPDK_NVME_ZONE_TYPE_SEQWR;
		zone_report->descs[i].za.raw = 0;
		zone_report->descs[i].zslba = (zone_idx+i) * detzone_ns->detzone_ns_bdev.zone_size;
		zone_report->descs[i].wp = detzone_ns->zone_info[zone_idx+i].write_pointer;
		zone_report->descs[i].zcap = detzone_ns->zone_info[zone_idx+i].capacity;
		switch (detzone_ns->zone_info[zone_idx+i].state) {
		case SPDK_BDEV_ZONE_STATE_EMPTY:
			zone_report->descs[i].zs = SPDK_NVME_ZONE_STATE_EMPTY;
			break;
		case SPDK_BDEV_ZONE_STATE_CLOSED:
			zone_report->descs[i].zs = SPDK_NVME_ZONE_STATE_CLOSED;
			break;
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
			zone_report->descs[i].zs = SPDK_NVME_ZONE_STATE_IOPEN;
			break;
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
			zone_report->descs[i].zs = SPDK_NVME_ZONE_STATE_EOPEN;
			break;
		case SPDK_BDEV_ZONE_STATE_FULL:
			zone_report->descs[i].zs = SPDK_NVME_ZONE_STATE_FULL;
			break;
		case SPDK_BDEV_ZONE_STATE_READ_ONLY:
			zone_report->descs[i].zs = SPDK_NVME_ZONE_STATE_RONLY;
			break;
		case SPDK_BDEV_ZONE_STATE_OFFLINE:
		default:
			zone_report->descs[i].zs = SPDK_NVME_ZONE_STATE_OFFLINE;
			break;
		}
		zone_report->nr_zones++;
	}

	return 0;
}

static void
_vbdev_detzone_ns_mgmt_lzone_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = cb_arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	uint64_t zone_idx;
	uint32_t cdw0 = 0;
	int sct = 0, sc = 0;

	if (!success) {
		mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
		spdk_bdev_io_get_nvme_status(bdev_io, &cdw0, &sct, &sc);
		spdk_bdev_io_complete_nvme_status(mgmt_io_ctx->parent_io, cdw0, sct, sc);
	}

	assert(mgmt_io_ctx->outstanding_mgmt_ios);
	mgmt_io_ctx->outstanding_mgmt_ios--;
	zone_idx = _vbdev_detzone_get_lzone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);

	if (mgmt_io_ctx->outstanding_mgmt_ios == 0 && mgmt_io_ctx->remain_ios == 0) {
		if (mgmt_io_ctx->status == SPDK_BDEV_IO_STATUS_SUCCESS) {
			detzone_ns->zone_info[zone_idx].state = detzone_ns->zone_info[zone_idx].next_state;
			if (detzone_ns->zone_info[zone_idx].state == SPDK_BDEV_ZONE_STATE_FULL) {
				detzone_ns->zone_info[zone_idx].write_pointer = detzone_ns->detzone_ns_bdev.zone_size;
			}
			if (mgmt_io_ctx->parent_io) {
				spdk_bdev_io_complete_nvme_status(mgmt_io_ctx->parent_io, 0, 0, 0);
				spdk_bdev_io_complete(mgmt_io_ctx->parent_io, SPDK_BDEV_IO_STATUS_SUCCESS);
			}
		} else {
			// this function will try to revert states and match to lzones
			//_vbdev_detzone_ns_lzone_state_recovery() 
			if (mgmt_io_ctx->parent_io) {
				spdk_bdev_io_complete(mgmt_io_ctx->parent_io, SPDK_BDEV_IO_STATUS_FAILED);
			}
		}
		free(mgmt_io_ctx);
	}
	spdk_bdev_free_io(bdev_io);
	return;
}

/**
 * @brief 
 * detzone_ctrlr has responsibility for ZONE allocation commands.
 * each namespace should send_msg to detzone_ctrlr thread using this.
 * once it completes the request, wake up the calling thread to resume I/O.
 */
 /*
static void
vbdev_detzone_reserve_zone(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = (struct vbdev_detzone_ns_mgmt_io_ctx *)arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	bool do_notification = false;
	uint64_t zone_idx;
	int i, rc;

	zone_idx = _vbdev_detzone_get_lzone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	if (detzone_ns->zone_info[zone_idx].next_state == SPDK_BDEV_ZONE_STATE_EMPTY
			 && detzone_ctrlr->thread != spdk_get_thread()) {
		spdk_thread_send_msg(detzone_ctrlr->thread, vbdev_detzone_ns_alloc_zone, arg);
		return;
	}

	assert(mgmt_io_ctx->parent_io);
	mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_GENERIC;
	mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_SUCCESS;
	
	detzone_ns->zone_info[zone_idx].next_state = SPDK_BDEV_ZONE_STATE_EXP_OPEN;
	detzone_ns->zone_info[zone_idx].pu_group = detzone_ctrlr->zone_alloc_cnt % detzone_ctrlr->num_pu;

	for (i=0; i < detzone_ns->zone_array_size; i++) {
		rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
						detzone_ns->zone_info[zone_idx].base_zone_id[i],
						mgmt_io_ctx->zone_mgmt.zone_action, _vbdev_detzone_ns_mgmt_lzone_cb, mgmt_io_ctx);
		if (rc != 0) {
			detzone_ns->zone_info[zone_idx].pu_group = detzone_ctrlr->num_pu;
			mgmt_io_ctx->remain_ios = 0;
			mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
			if (i == 0) {
				// we can simply cancel the transition for this lzone since no command has been submitted.
				detzone_ns->zone_info[zone_idx].next_state = detzone_ns->zone_info[zone_idx].state;
			}
			if (mgmt_io_ctx->outstanding_mgmt_ios) {
				// TODO: Have to consider various error cases.
				// We may reset opened zones to release the resource.
				// However, we don't know if the SSD F/W reuses those released zones immediately or later.
				// This make decreasing zone_alloc_cnt be complicated.
				// Later, we have to check the current physical state and detect erroneous situation in advance. 
				mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_INTERNAL_DEVICE_ERROR;
				rc = 0;		// error will be handled by completing ios
			}
			return;
		}
		detzone_ctrlr->zone_alloc_cnt++;
		mgmt_io_ctx->outstanding_mgmt_ios++;
		mgmt_io_ctx->remain_ios--;
	}
}
*/
static void
vbdev_detzone_ns_alloc_zone(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = (struct vbdev_detzone_ns_mgmt_io_ctx *)arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	//bool do_notification = false;
	uint64_t zone_idx;
	uint32_t i;
	int rc;

	zone_idx = _vbdev_detzone_get_lzone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	if (detzone_ns->zone_info[zone_idx].next_state == SPDK_BDEV_ZONE_STATE_EMPTY
			 && detzone_ctrlr->thread != spdk_get_thread()) {
		spdk_thread_send_msg(detzone_ctrlr->thread, vbdev_detzone_ns_alloc_zone, arg);
		return;
	}

	assert(mgmt_io_ctx->parent_io);
	mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_GENERIC;
	mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_SUCCESS;
	
	detzone_ns->zone_info[zone_idx].next_state = SPDK_BDEV_ZONE_STATE_EXP_OPEN;
	detzone_ns->zone_info[zone_idx].pu_group = detzone_ctrlr->zone_alloc_cnt % detzone_ctrlr->num_pu;

	for (i=0; i < detzone_ns->zone_array_size; i++) {
		rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
						detzone_ns->zone_info[zone_idx].base_zone_id[i],
						mgmt_io_ctx->zone_mgmt.zone_action, _vbdev_detzone_ns_mgmt_lzone_cb, mgmt_io_ctx);
		if (rc != 0) {
			detzone_ns->zone_info[zone_idx].pu_group = detzone_ctrlr->num_pu;
			mgmt_io_ctx->remain_ios = 0;
			mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
			if (i == 0) {
				// we can simply cancel the transition for this lzone since no command has been submitted.
				detzone_ns->zone_info[zone_idx].next_state = detzone_ns->zone_info[zone_idx].state;
			}
			if (mgmt_io_ctx->outstanding_mgmt_ios) {
				// TODO: Have to consider various error cases.
				// We may reset opened zones to release the resource.
				// However, we don't know if the SSD F/W reuses those released zones immediately or later.
				// This make decreasing zone_alloc_cnt be complicated.
				// Later, we have to check the current physical state and detect erroneous situation in advance. 
				mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_INTERNAL_DEVICE_ERROR;
				rc = 0;		// error will be handled by completing ios
			}
			return;
		}
		detzone_ctrlr->zone_alloc_cnt++;
		mgmt_io_ctx->outstanding_mgmt_ios++;
		mgmt_io_ctx->remain_ios--;
	}
}

static void
vbdev_detzone_ns_lzone_notify(struct spdk_io_channel_iter *i)
{
	struct spdk_io_channel *ch = spdk_io_channel_iter_get_channel(i);
	//struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);

	vbdev_detzone_slidewin_resume(ch);
	spdk_for_each_channel_continue(i, 0);

}

static void
vbdev_detzone_get_zone_resource(void *arg)
{
	struct vbdev_detzone_ns *detzone_ns = (struct vbdev_detzone_ns *)arg;
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	bool do_notification = false;

	if (detzone_ctrlr->thread != spdk_get_thread()) {
		spdk_thread_send_msg(detzone_ctrlr->thread, vbdev_detzone_get_zone_resource, arg);
		return;
	}

	pthread_spin_lock(&detzone_ns->internal.lock);
	switch (detzone_ns->internal.ns_state) {
	case VBDEV_DETZONE_NS_STATE_ACTIVE:
		break;
	default:
		if (detzone_ctrlr->num_open_states + detzone_ns->zone_array_size
						<= detzone_ctrlr->base_bdev->max_open_zones) {
			detzone_ctrlr->num_open_states += detzone_ns->zone_array_size;
			if (detzone_ns->internal.ns_state == VBDEV_DETZONE_NS_STATE_PENDING) {
				TAILQ_REMOVE(&detzone_ctrlr->ns_pending, detzone_ns, state_link);
			}
			detzone_ns->internal.ns_state = VBDEV_DETZONE_NS_STATE_ACTIVE;
			do_notification = true;
		} else if (detzone_ns->internal.ns_state == VBDEV_DETZONE_NS_STATE_CLOSE) {
			detzone_ns->internal.ns_state = VBDEV_DETZONE_NS_STATE_PENDING;
			TAILQ_INSERT_TAIL(&detzone_ctrlr->ns_active, detzone_ns, state_link);
		}
		break;
	}
	pthread_spin_unlock(&detzone_ns->internal.lock);
	if (do_notification) {
		spdk_for_each_channel(detzone_ns, vbdev_detzone_ns_lzone_notify, NULL, NULL);
	}
}

static void
vbdev_detzone_put_zone_resource(void *arg)
{
	struct vbdev_detzone_ns *detzone_ns = (struct vbdev_detzone_ns *)arg;
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	struct vbdev_detzone_ns *ns, *tmp_ns;

	if (detzone_ctrlr->thread != spdk_get_thread()) {
		spdk_thread_send_msg(detzone_ctrlr->thread, vbdev_detzone_put_zone_resource, arg);
		return;
	}

	TAILQ_FOREACH_SAFE(ns, &detzone_ctrlr->ns_active, state_link, tmp_ns) {
		if (ns == detzone_ns) {
			pthread_spin_lock(&detzone_ns->internal.lock);
			detzone_ctrlr->num_open_states -= detzone_ns->zone_array_size;
			ns->internal.ns_state = VBDEV_DETZONE_NS_STATE_CLOSE;
			pthread_spin_unlock(&detzone_ns->internal.lock);

			TAILQ_REMOVE(&detzone_ctrlr->ns_active, ns, state_link);

			// try to wake up pending namespace
			spdk_thread_send_msg(detzone_ctrlr->thread,
					 vbdev_detzone_get_zone_resource, TAILQ_FIRST(&detzone_ctrlr->ns_pending));
			return;
		}
	}
}

static bool
vbdev_detzone_ns_is_active(void *arg)
{
	struct vbdev_detzone_ns *detzone_ns = (struct vbdev_detzone_ns *)arg;
	bool ret;
	
	pthread_spin_lock(&detzone_ns->internal.lock);
	ret = (detzone_ns->internal.ns_state == VBDEV_DETZONE_NS_STATE_ACTIVE) ?
			1 : 0;
	pthread_spin_unlock(&detzone_ns->internal.lock);
	return ret;
}

static int
vbdev_detzone_ns_exp_open_lzone(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io,
								 uint64_t lzslba, bool sel_all)
{
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns,
					 detzone_ns_bdev);
	//struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	//struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);
	//struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx;
	uint64_t zone_idx;
	int sct = SPDK_NVME_SCT_GENERIC, sc = SPDK_NVME_SC_SUCCESS;

	if (bdev_io->type != SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT) {
		return -EINVAL;
	} else if (sel_all ||
			detzone_ns->num_open_lzones >= detzone_ns->detzone_ns_bdev.max_open_zones) {
		// Each detzone namespace has only one active lzone.
		// Thus, zone open command that 'Select All' bit is set to 1 always fail.
		sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
		sc = SPDK_NVME_SC_ZONE_TOO_MANY_OPEN;
		goto error_complete;
	}

	if (lzslba % detzone_ns->detzone_ns_bdev.zone_size ||
			lzslba >= detzone_ns->detzone_ns_bdev.blockcnt) {
		sc = SPDK_NVME_SC_INVALID_FIELD;
		goto error_complete;
	}
	zone_idx = _vbdev_detzone_get_lzone_idx(detzone_ns, lzslba);
	switch (detzone_ns->zone_info[zone_idx].state) {
	case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		detzone_ns->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_EXP_OPEN;
		spdk_bdev_io_complete_nvme_status(bdev_io, 0, sct, sc);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
		return 0;
	case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
		// NOOP
		spdk_bdev_io_complete_nvme_status(bdev_io, 0, sct, sc);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
		return 0;
	case SPDK_BDEV_ZONE_STATE_FULL:
		sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
		sc = SPDK_NVME_SC_ZONE_INVALID_STATE;
		goto error_complete;
	case SPDK_BDEV_ZONE_STATE_READ_ONLY:
		sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
		sc = SPDK_NVME_SC_ZONE_IS_READONLY;
		goto error_complete;
	case SPDK_BDEV_ZONE_STATE_OFFLINE:
		sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
		sc = SPDK_NVME_SC_ZONE_IS_OFFLINE;
		goto error_complete;
	//case SPDK_BDEV_ZONE_STATE_EMPTY:
	//case SPDK_BDEV_ZONE_STATE_CLOSED:
	default:
		break;
	}

	mgmt_io_ctx = calloc(sizeof(struct vbdev_detzone_ns_mgmt_io_ctx), 1);
	if (mgmt_io_ctx) {
		return -ENOMEM;
	}
	mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	mgmt_io_ctx->parent_io = bdev_io;
	mgmt_io_ctx->detzone_ns = detzone_ns;
	mgmt_io_ctx->remain_ios = detzone_ns->zone_array_size;
	mgmt_io_ctx->outstanding_mgmt_ios = 0;
	
	mgmt_io_ctx->zone_mgmt.zone_id = lzslba;
	mgmt_io_ctx->zone_mgmt.zone_action = SPDK_BDEV_ZONE_OPEN;

	vbdev_detzone_ns_alloc_zone(mgmt_io_ctx);
	return 0;

error_complete:
	spdk_bdev_io_complete_nvme_status(bdev_io, 0, sct, sc);
	spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	return 0;
}

static int
vbdev_detzone_ns_imp_open_lzone(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io,
									struct vbdev_detzone_ns *detzone_ns, uint64_t lzslba)
{
	//struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	//struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx;
	uint64_t zone_idx;

	if (bdev_io->type == SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT) {
		return -EINVAL;
	}
	assert(lzslba % detzone_ns->detzone_ns_bdev.zone_size == 0);
	assert(lzslba < detzone_ns->detzone_ns_bdev.blockcnt);

	zone_idx = _vbdev_detzone_get_lzone_idx(detzone_ns, lzslba);
	switch (detzone_ns->zone_info[zone_idx].state) {
	case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
	case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
		// NOOP
		return 0;
	case SPDK_BDEV_ZONE_STATE_CLOSED:
	case SPDK_BDEV_ZONE_STATE_EMPTY:
		break;
	default:
		// any other states will generate errors in actual I/O execution
		return 0;
	}

	mgmt_io_ctx = calloc(sizeof(struct vbdev_detzone_ns_mgmt_io_ctx), 1);
	if (mgmt_io_ctx) {
		return -ENOMEM;
	}
	mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	mgmt_io_ctx->parent_io = bdev_io;
	mgmt_io_ctx->detzone_ns = detzone_ns;
	mgmt_io_ctx->remain_ios = detzone_ns->zone_array_size;
	mgmt_io_ctx->outstanding_mgmt_ios = 0;

	mgmt_io_ctx->zone_mgmt.zone_id = lzslba;
	mgmt_io_ctx->zone_mgmt.zone_action = SPDK_BDEV_ZONE_OPEN;

	vbdev_detzone_ns_alloc_zone(mgmt_io_ctx);
	return -EAGAIN;
}

static int
vbdev_detzone_ns_reset_lzone(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io,
							  uint64_t lzslba, bool sel_all)
{
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns,
					 detzone_ns_bdev);
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx;
	uint64_t zone_idx, num_left_zones = 1;
	uint32_t i;
	uint32_t cdw0 = 0;
	int sct = SPDK_NVME_SCT_GENERIC, sc = SPDK_NVME_SC_SUCCESS;
	int rc;

	if (bdev_io->type != SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT) {
		return -EINVAL;
	} else if (sel_all) {
		lzslba = 0;
		num_left_zones = detzone_ns->detzone_ns_bdev.blockcnt / detzone_ns->detzone_ns_bdev.zone_size;
	} else if (lzslba % detzone_ns->detzone_ns_bdev.zone_size ||
			lzslba >= detzone_ns->detzone_ns_bdev.blockcnt) {
		sc = SPDK_NVME_SC_INVALID_FIELD;
		goto error_complete;
	}

	zone_idx = _vbdev_detzone_get_lzone_idx(detzone_ns, lzslba);

	if (!sel_all) {
		switch (detzone_ns->zone_info[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_READ_ONLY:
			sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			sc = SPDK_NVME_SC_ZONE_IS_READONLY;
			goto error_complete;
		case SPDK_BDEV_ZONE_STATE_OFFLINE:
			sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			sc = SPDK_NVME_SC_ZONE_IS_OFFLINE;
			goto error_complete;
		case SPDK_BDEV_ZONE_STATE_EMPTY:
			spdk_bdev_io_complete_nvme_status(bdev_io, cdw0, sct, sc);
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
			return 0;
		default:
			break;
		}
	}

	mgmt_io_ctx = calloc(sizeof(struct vbdev_detzone_ns_mgmt_io_ctx), 1);
	if (mgmt_io_ctx) {
		return -ENOMEM;
	}
	mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	mgmt_io_ctx->parent_io = (bdev_io->type == SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT) ? bdev_io : NULL;
	mgmt_io_ctx->detzone_ns = detzone_ns;
	mgmt_io_ctx->remain_ios = num_left_zones * detzone_ns->zone_array_size;
	mgmt_io_ctx->outstanding_mgmt_ios = 0;

	while (mgmt_io_ctx->remain_ios > 0) {
		switch (detzone_ns->zone_info[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
		case SPDK_BDEV_ZONE_STATE_CLOSED:
		case SPDK_BDEV_ZONE_STATE_FULL:
			detzone_ns->zone_info[zone_idx].next_state = SPDK_BDEV_ZONE_STATE_EMPTY;
			for (i = 0; i < detzone_ns->zone_array_size; i++) {
				rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ch->base_ch,
								detzone_ns->zone_info[zone_idx].base_zone_id[i],
								SPDK_BDEV_ZONE_RESET, _vbdev_detzone_ns_mgmt_lzone_cb, mgmt_io_ctx);
				if (rc != 0) {
					mgmt_io_ctx->remain_ios = 0;
					mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
					if (i == 0) {
						// we can simply cancel the transition for this lzone since no command has been submitted.
						detzone_ns->zone_info[zone_idx].next_state = detzone_ns->zone_info[zone_idx].state;
					}
					if (mgmt_io_ctx->outstanding_mgmt_ios) {
						sc = SPDK_NVME_SC_INTERNAL_DEVICE_ERROR;
						spdk_bdev_io_complete_nvme_status(bdev_io, cdw0, sct, sc);
						rc = 0;		// error will be handled by completing ios
					}
					return rc;
				}
				mgmt_io_ctx->outstanding_mgmt_ios++;
			}
		default:
			// skip zones in these states
			break;;
		}
		zone_idx++;
		mgmt_io_ctx->remain_ios -= detzone_ns->zone_array_size;
	}

	return 0;

error_complete:
	spdk_bdev_io_complete_nvme_status(bdev_io, cdw0, sct, sc);
	spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	return 0;
}

static int
vbdev_detzone_ns_close_lzone(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io,
							  uint64_t lzslba, bool sel_all)
{
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns,
					 detzone_ns_bdev);
	//struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	uint64_t zone_idx, num_left_zones = 1;
	uint32_t cdw0 = 0;
	int sct = SPDK_NVME_SCT_GENERIC, sc = SPDK_NVME_SC_SUCCESS;

	if (bdev_io->type != SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT) {
		return -EINVAL;
	} else if (sel_all) {
		lzslba = 0;
		num_left_zones = detzone_ns->detzone_ns_bdev.blockcnt / detzone_ns->detzone_ns_bdev.zone_size;
	} else if (lzslba % detzone_ns->detzone_ns_bdev.zone_size ||
			lzslba >= detzone_ns->detzone_ns_bdev.blockcnt) {
		sc = SPDK_NVME_SC_INVALID_FIELD;
		goto error_complete;
	}
	
	zone_idx = _vbdev_detzone_get_lzone_idx(detzone_ns, lzslba);
	while (num_left_zones) {
		switch (detzone_ns->zone_info[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
			detzone_ns->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_CLOSED;
		case SPDK_BDEV_ZONE_STATE_CLOSED:
			break;

		default:
			if (sel_all) {
				break;
			} else {
				sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
				sc = SPDK_NVME_SC_ZONE_INVALID_STATE;
				goto error_complete;
			}
		}
		zone_idx++;
		num_left_zones--;
	}

	spdk_bdev_io_complete_nvme_status(bdev_io, cdw0, sct, sc);
	spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
	return 0;

error_complete:
	spdk_bdev_io_complete_nvme_status(bdev_io, cdw0, sct, sc);
	spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	return 0;
}

static int
vbdev_detzone_finish_lzone(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io,
							  uint64_t lzslba, bool sel_all)
{
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns,
					 detzone_ns_bdev);
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx;
	uint64_t zone_idx, num_left_zones = 1;
	uint32_t i;
	uint32_t cdw0 = 0;
	int sct = SPDK_NVME_SCT_GENERIC, sc = SPDK_NVME_SC_SUCCESS;
	int rc;

	if (bdev_io->type != SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT) {
		return -EINVAL;
	} else if (sel_all) {
		lzslba = 0;
		num_left_zones = detzone_ns->detzone_ns_bdev.blockcnt / detzone_ns->detzone_ns_bdev.zone_size;
	} else if (lzslba % detzone_ns->detzone_ns_bdev.zone_size ||
			lzslba >= detzone_ns->detzone_ns_bdev.blockcnt) {
		sc = SPDK_NVME_SC_INVALID_FIELD;
		goto error_complete;
	}

	zone_idx = _vbdev_detzone_get_lzone_idx(detzone_ns, lzslba);

	if (!sel_all) {
		switch (detzone_ns->zone_info[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_READ_ONLY:
			sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			sc = SPDK_NVME_SC_ZONE_IS_READONLY;
			goto error_complete;
		case SPDK_BDEV_ZONE_STATE_OFFLINE:
			sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			sc = SPDK_NVME_SC_ZONE_IS_OFFLINE;
			goto error_complete;
		case SPDK_BDEV_ZONE_STATE_FULL:
			spdk_bdev_io_complete_nvme_status(bdev_io, cdw0, sct, sc);
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
			return 0;
		default:
			break;
		}
	}

	mgmt_io_ctx = calloc(sizeof(struct vbdev_detzone_ns_mgmt_io_ctx), 1);
	if (mgmt_io_ctx) {
		return -ENOMEM;
	}
	mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	mgmt_io_ctx->parent_io = (bdev_io->type == SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT) ? bdev_io : NULL;
	mgmt_io_ctx->detzone_ns = detzone_ns;
	mgmt_io_ctx->remain_ios = num_left_zones * detzone_ns->zone_array_size;
	mgmt_io_ctx->outstanding_mgmt_ios = 0;

	while (mgmt_io_ctx->remain_ios > 0) {
		switch (detzone_ns->zone_info[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
		case SPDK_BDEV_ZONE_STATE_CLOSED:
			detzone_ns->zone_info[zone_idx].next_state = SPDK_BDEV_ZONE_STATE_FULL;
			for (i = 0; i < detzone_ns->zone_array_size; i++) {
				rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ch->base_ch,
								detzone_ns->zone_info[zone_idx].base_zone_id[i],
								SPDK_BDEV_ZONE_FINISH, _vbdev_detzone_ns_mgmt_lzone_cb, mgmt_io_ctx);
				if (rc != 0) {
					mgmt_io_ctx->remain_ios = 0;
					mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
					if (i == 0) {
						// we can simply cancel the transition for this lzone since no command has been submitted.
						detzone_ns->zone_info[zone_idx].next_state = detzone_ns->zone_info[zone_idx].state;
					}
					if (mgmt_io_ctx->outstanding_mgmt_ios) {
						sc = SPDK_NVME_SC_INTERNAL_DEVICE_ERROR;
						spdk_bdev_io_complete_nvme_status(bdev_io, cdw0, sct, sc);
						rc = 0;		// error will be handled by completing ios
					}
					return rc;
				}
				mgmt_io_ctx->outstanding_mgmt_ios++;
			}
		default:
			// skip zones in other states
			break;;
		}
		zone_idx++;
		mgmt_io_ctx->remain_ios -= detzone_ns->zone_array_size;
	}

	return 0;

error_complete:
	spdk_bdev_io_complete_nvme_status(bdev_io, cdw0, sct, sc);
	spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	return 0;
}

static int
_detzone_ns_io_submit(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns,
					 detzone_ns_bdev);
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;

	int rc = 0;
	uint64_t io_offset_blocks;
	uint64_t blks_to_submit;
	int iov_idx;
	uint64_t iov_offset;
	uint64_t zone_idx;

	iov_idx = 0;
	iov_offset = 0;

	while (io_ctx->remain_blocks) {
		// TODO: use '&' operator rather than '%'
		// TODO: not here, but we have to check if the stripe size is a factor of physical zone at the init phase
		if (io_ctx->next_offset_blocks % detzone_ns->detzone_ns_bdev.zone_size > detzone_ns->zcap) {
			if (bdev_io->type == SPDK_BDEV_IO_TYPE_WRITE) {
				return -EINVAL; // TODO: this should be checked before entering this cb function
			}
			// Currently, we return zeroes for a range beyond ZCAP (SPDK_NVME_DEALLOC_READ_00)
			// TODO: behavior should match with the device (i.e., DLFEAT bit)
			blks_to_submit = spdk_min(detzone_ns->detzone_ns_bdev.zone_size - 
							 (io_ctx->next_offset_blocks % detzone_ns->detzone_ns_bdev.zone_size),
							 bdev_io->u.bdev.iovs[iov_idx].iov_len - iov_offset);
			if (spdk_unlikely(blks_to_submit > io_ctx->remain_blocks)) {
				blks_to_submit = io_ctx->remain_blocks;
			}
			memset(bdev_io->u.bdev.iovs[iov_idx].iov_base + iov_offset,
						 SPDK_NVME_DEALLOC_READ_00, blks_to_submit);
			rc = 0;
		} else {
			blks_to_submit = spdk_min(vbdev_detzone_slidewin_avail(io_ctx), io_ctx->remain_blocks);
			if (blks_to_submit == 0) {
				rc = -EAGAIN;
				break;
			}
			io_offset_blocks = _vbdev_detzone_get_base_offset(detzone_ns, io_ctx->next_offset_blocks);
			blks_to_submit = spdk_min(blks_to_submit, 
							detzone_ns->stripe_blocks - (io_offset_blocks % detzone_ns->stripe_blocks));
			
			// We reuse allocated iovs instead of trying to get new one. 
			// It is likely aligned with the stripes
			if (spdk_unlikely(blks_to_submit > bdev_io->u.bdev.iovs[iov_idx].iov_len - iov_offset)) {
				blks_to_submit = bdev_io->u.bdev.iovs[iov_idx].iov_len - iov_offset;
			}

			// TODO: check if this namespace blocklen is not equal to the base blocklen.
			// detzone_ns_bdev.phys_blocklen != detzone_ns_bdev.blocklen
			// If so, convert it here
			//printf("id:%u length:%lu org_offset:%lu phy_offset:%lu\n", detzone_ch->ch_id, bdev_io->u.bdev.num_blocks, bdev_io->u.bdev.offset_blocks, io_offset_blocks);
			//printf("next_offset_blocks: %lu remain_blocks: %lu\n", io_ctx->next_offset_blocks, io_ctx->remain_blocks);
			//printf("zone_size: %lu stripe_blocks: %u zone_array_size: %u\n", detzone_ns->detzone_ns_bdev.zone_size, detzone_ns->stripe_blocks, detzone_ns->zone_array_size);
			//printf("offset: %lu org_offset:%lu org_blocks:%lu\n", io_offset_blocks, bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);

			assert(blks_to_submit>0);

			switch (bdev_io->type) {
			case SPDK_BDEV_IO_TYPE_READ:
				rc = spdk_bdev_read_blocks(detzone_ctrlr->base_desc, detzone_ch->base_ch,
								bdev_io->u.bdev.iovs[iov_idx].iov_base + iov_offset,
								io_offset_blocks, blks_to_submit, _detzone_ns_complete_io,
								bdev_io);
				break;
			case SPDK_BDEV_IO_TYPE_WRITE:
				if (io_ctx->next_offset_blocks % detzone_ns->detzone_ns_bdev.zone_size == 0) {
					// Open a logical zone
					if (detzone_ctrlr->thread != spdk_get_thread()) {
						// TODO: send_msg
					} else {
						rc = vbdev_detzone_ns_imp_open_lzone(ch, bdev_io, detzone_ns, io_ctx->next_offset_blocks);
						if (rc != 0) {
							break;
						}
					}
				}

				zone_idx = _vbdev_detzone_get_lzone_idx(detzone_ns, io_ctx->next_offset_blocks);
				if (spdk_unlikely(detzone_ns->zone_info[zone_idx].state != SPDK_BDEV_ZONE_STATE_EXP_OPEN &&
				     detzone_ns->zone_info[zone_idx].state != SPDK_BDEV_ZONE_STATE_IMP_OPEN)) {
						rc = -EAGAIN;
						break;
				}

				rc = spdk_bdev_write_blocks(detzone_ctrlr->base_desc, detzone_ch->base_ch,
								bdev_io->u.bdev.iovs[iov_idx].iov_base + iov_offset,
								io_offset_blocks, blks_to_submit, _detzone_ns_complete_io,
								bdev_io);
				break;
			case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
				// TODO : we need to handle WRITE_ZEROES separately.
				//rc = spdk_bdev_write_zeroes_blocks(detzone_ctrlr->base_desc, detzone_ch->base_ch,
				//				io_offset_blocks, blks_to_submit,
				//				_detzone_ns_complete_io, bdev_io);
				rc = -EINVAL;
				break;
			case SPDK_BDEV_IO_TYPE_ZONE_APPEND:
				// TODO : we will support ZONE_APPEND but only for queueing and reordering purpose in the scheduler.
				// thus, underlying base_bdev doesn't have to have the feature.
				rc = -EINVAL;
				break;
			default:
				rc = -EINVAL;
				SPDK_ERRLOG("detzone: unknown I/O type %d\n", bdev_io->type);
				break;

			}

			if (rc == 0) {
				io_ctx->outstanding_stripe_ios++;
				vbdev_detzone_slidewin_submit(io_ctx, blks_to_submit);
			}
		}

		if (rc == -ENOMEM) {
			SPDK_ERRLOG("No memory, start to queue io for delay.\n");
			if (vbdev_detzone_queue_io(bdev_io) != 0) {
				goto error_out;
			}
		} else if (rc != 0) {
			SPDK_ERRLOG("ERROR on bdev_io submission!\n");
			goto error_out;
		} else {
			// TODO: trace each I/O latency separately
			// define child io array in io_ctx -> set latency tsc for each child io -> pass it as cb_arg for _detzone_ns_complete_io
			//io_ctx->submit_tick = spdk_get_ticks();
			io_ctx->next_offset_blocks += blks_to_submit;
			io_ctx->remain_blocks -= blks_to_submit;

			iov_offset += blks_to_submit;
			if (iov_offset == bdev_io->u.bdev.iovs[iov_idx].iov_len) {
				iov_offset = 0;
				iov_idx++;
			}
		}
	}

	if (rc == -EAGAIN) {
		vbdev_detzone_slidewin_requeue(io_ctx);
	}
	return 0;

error_out:
	io_ctx->remain_blocks = 0;
	io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
	if (io_ctx->outstanding_stripe_ios != 0) {
		// defer error handling until all stripe ios complete
		rc = 0;
	}
	return rc;
}

static void
//_detzone_ns_read_get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io, bool success)
_detzone_ns_rw_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io, bool success)
{
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;

	if (!success) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
	
	if (vbdev_detzone_slidewin_empty(io_ctx)) {
		if (_detzone_ns_io_submit(ch, bdev_io) != 0) {
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
	} else {
		vbdev_detzone_slidewin_enqueue(io_ctx);	
	}
	return;
}

static void
vbdev_detzone_reset_dev(struct spdk_io_channel_iter *i, int status)
{
	struct spdk_bdev_io *bdev_io = spdk_io_channel_iter_get_ctx(i);
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(io_ctx->ch);
	struct vbdev_detzone *detzone_ctrlr = spdk_io_channel_iter_get_io_device(i);
	int rc;

	rc = spdk_bdev_reset(detzone_ctrlr->base_desc, detzone_ch->base_ch,
			     _detzone_complete_mgmt, bdev_io);

	if (rc == -ENOMEM) {
		SPDK_ERRLOG("No memory, start to queue io for delay.\n");
		vbdev_detzone_queue_io(bdev_io);
	} else if (rc != 0) {
		SPDK_ERRLOG("ERROR on bdev_io submission!\n");
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	} else {
		io_ctx->outstanding_stripe_ios++;
	}
}

static void
_abort_all_queued_io(void *arg)
{
	TAILQ_HEAD(, detzone_bdev_io) *head = arg;
	struct detzone_bdev_io *io_ctx, *tmp;

	TAILQ_FOREACH_SAFE(io_ctx, head, link, tmp) {
		TAILQ_REMOVE(head, io_ctx, link);
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(io_ctx), SPDK_BDEV_IO_STATUS_ABORTED);
	}
}

static void
vbdev_detzone_reset_channel(struct spdk_io_channel_iter *i)
{
	struct spdk_io_channel *ch = spdk_io_channel_iter_get_channel(i);
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);

	_abort_all_queued_io(&detzone_ch->rd_slidewin_queue);
	_abort_all_queued_io(&detzone_ch->wr_slidewin_queue);
	_abort_all_queued_io(&detzone_ch->write_drr_queue);

	spdk_for_each_channel_continue(i, 0);
}

static int
_abort_queued_io(void *_head, struct spdk_bdev_io *bio_to_abort)
{
	TAILQ_HEAD(, detzone_bdev_io) *head = _head;
	struct detzone_bdev_io *io_ctx_to_abort = (struct detzone_bdev_io *)bio_to_abort->driver_ctx;
	struct detzone_bdev_io *io_ctx;

	TAILQ_FOREACH(io_ctx, head, link) {
		if (io_ctx == io_ctx_to_abort) {
			TAILQ_REMOVE(head, io_ctx, link);
			if (io_ctx->outstanding_stripe_ios == 0 &&
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
vbdev_detzone_abort(struct vbdev_detzone *detzone_ctrlr, struct detzone_io_channel *detzone_ch,
		  struct spdk_bdev_io *bdev_io)
{
	struct spdk_bdev_io *bio_to_abort = bdev_io->u.abort.bio_to_abort;
	struct detzone_bdev_io *io_ctx_to_abort = (struct detzone_bdev_io *)bio_to_abort->driver_ctx;

	if (_abort_queued_io(&detzone_ch->rd_slidewin_queue, bio_to_abort) == 0 ||
			_abort_queued_io(&detzone_ch->wr_slidewin_queue, bio_to_abort) == 0 || 
			_abort_queued_io(&detzone_ch->write_drr_queue, bio_to_abort) == 0) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
		return 0;
	} else if (io_ctx_to_abort->type == DETZONE_IO_MGMT) {
		return spdk_bdev_abort(detzone_ctrlr->base_desc, detzone_ch->base_ch, bio_to_abort,
					_detzone_complete_mgmt, bdev_io);
	} else {
		return -ENOENT;
	}
}

/* We currently don't support a normal I/O command in detzone_mgmt bdev.
 *  detzone_mgmt is only used for internal management and creating virtual namespace.
 */
static void
vbdev_detzone_mgmt_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	SPDK_ERRLOG("detzone: mgmt does not support a normal I/O type %d\n", bdev_io->type);
	spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	return;
}

static uint64_t
detzone_ns_get_zone_info(struct vbdev_detzone_ns *detzone_ns, uint64_t slba,
							 uint32_t num_zones, struct spdk_bdev_zone_info *info)
{
	uint64_t zone_idx, num_lzones;
	uint32_t i;
	zone_idx = _vbdev_detzone_get_lzone_idx(detzone_ns, slba);
	num_lzones = detzone_ns->detzone_ns_bdev.blockcnt / detzone_ns->detzone_ns_bdev.zone_size;

	for (i=0; i < num_zones && zone_idx + i < num_lzones; i++) {
		info[i].zone_id = (zone_idx + i) * detzone_ns->detzone_ns_bdev.zone_size;
		info[i].write_pointer = detzone_ns->zone_info[zone_idx + i].write_pointer;
		info[i].capacity = detzone_ns->zone_info[zone_idx + i].capacity;
		info[i].state = detzone_ns->zone_info[zone_idx + i].state;
	}
	return i;
}

static int
_detzone_ns_mgmt_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns, detzone_ns_bdev);
	struct vbdev_detzone	 *detzone_ctrlr = detzone_ns->ctrl;
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	int rc = 0;

	io_ctx->type = DETZONE_IO_MGMT;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_GET_ZONE_INFO:
		detzone_ns_get_zone_info(detzone_ns, bdev_io->u.zone_mgmt.zone_id,
					 bdev_io->u.zone_mgmt.num_zones, bdev_io->u.zone_mgmt.buf);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
		break;
	case SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT:
		rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ch->base_ch,
						bdev_io->u.zone_mgmt.zone_id, bdev_io->u.zone_mgmt.zone_action,
						_detzone_complete_mgmt, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_UNMAP:
		rc = spdk_bdev_unmap_blocks(detzone_ctrlr->base_desc, detzone_ch->base_ch,
					    bdev_io->u.bdev.offset_blocks,
					    bdev_io->u.bdev.num_blocks,
					    _detzone_complete_mgmt, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_FLUSH:
		rc = spdk_bdev_flush_blocks(detzone_ctrlr->base_desc, detzone_ch->base_ch,
					    bdev_io->u.bdev.offset_blocks,
					    bdev_io->u.bdev.num_blocks,
					    _detzone_complete_mgmt, bdev_io);
		break;
	default:
		rc = -EINVAL;
		SPDK_ERRLOG("detzone: unknown I/O type %d\n", bdev_io->type);
		break;
	}
	//printf("_detzone_ns_mgmt_submit_request: %d\n", rc);
	if (rc == 0) {
		io_ctx->outstanding_stripe_ios++;
	}
	return rc;
}

static void
vbdev_detzone_ns_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns, detzone_ns_bdev);
	struct vbdev_detzone	 *detzone_ctrlr = detzone_ns->ctrl;
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	int rc = 0;

	io_ctx->ch = ch;
	io_ctx->outstanding_stripe_ios = 0;
	io_ctx->status = SPDK_BDEV_IO_STATUS_PENDING;
		
	switch (bdev_io->type) {
	/*
	case SPDK_BDEV_IO_TYPE_READ:
		io_ctx->type = DETZONE_IO_READ;
		if ((rc = _detzone_cong_token_get(detzone_ch, io_ctx->type,
						bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen)) < 0) {
			break;
		} else {
			rc = 0;
			spdk_bdev_io_get_buf(bdev_io, detzone_read_get_buf_cb,
						bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		}
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		io_ctx->type = DETZONE_IO_WRITE;
		if ((rc = _detzone_cong_token_get(detzone_ch, io_ctx->type,
						bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen)) < 0) {
			break;
		} else {
			rc = spdk_bdev_writev_blocks(detzone_ctrlr->base_desc, detzone_ch->base_ch, bdev_io->u.bdev.iovs,
							bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.offset_blocks,
							bdev_io->u.bdev.num_blocks, _detzone_ns_complete_io,
							bdev_io);
		}
		break;
	*/

	// Try to abort I/O if it is a R/W I/O in congestion queues or management command.
	// We cannot abort R/W I/Os already in progress because we may split them.
	case SPDK_BDEV_IO_TYPE_ABORT:
		rc = vbdev_detzone_abort(detzone_ctrlr, detzone_ch, bdev_io);
		break;

	// TODO: We may need a special handling for ZONE_RESET in the high capacity utilization because it may impact others.
	// TODO: We need a special handling for ZONE_OPEN/CLOSE for striped zones.
	//case SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT:

	// TODO: We may flush I/O schueduler queues for a detzone_ns when FLUSH is submitted.
	//       FLUSH may be suspended until all I/O commands in queues complete to emulate FLUSH operation.
	//case SPDK_BDEV_IO_TYPE_FLUSH:

	case SPDK_BDEV_IO_TYPE_RESET:
		// For SPDK_BDEV_IO_TYPE_RESET, we shall abort all I/Os in the scheduling queue before
		// processing delayed I/Os in underlying layers.
		_abort_all_queued_io(&detzone_ch->rd_slidewin_queue);
		_abort_all_queued_io(&detzone_ch->wr_slidewin_queue);
		_abort_all_queued_io(&detzone_ch->write_drr_queue);
		/* During reset, the generic bdev layer aborts all new I/Os and queues all new resets.
		 * Hence we can simply abort all I/Os delayed to complete.
		 */
		spdk_for_each_channel(detzone_ctrlr, vbdev_detzone_reset_channel, bdev_io,
				      vbdev_detzone_reset_dev);
		break;

	// READ
	// TODO : Sliding window scheduling
	case SPDK_BDEV_IO_TYPE_READ:
		io_ctx->type = DETZONE_IO_READ;
		io_ctx->remain_blocks = bdev_io->u.bdev.num_blocks;
		io_ctx->next_offset_blocks = bdev_io->u.bdev.offset_blocks;
		spdk_bdev_io_get_buf(bdev_io, _detzone_ns_rw_cb,
					bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		break;

	// WRITE
	// TODO : DRR scheduling
	case SPDK_BDEV_IO_TYPE_WRITE:
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
	case SPDK_BDEV_IO_TYPE_ZONE_APPEND:
		io_ctx->type = DETZONE_IO_WRITE;
		io_ctx->remain_blocks = bdev_io->u.bdev.num_blocks;
		io_ctx->next_offset_blocks = bdev_io->u.bdev.offset_blocks;
		_detzone_ns_rw_cb(ch, bdev_io, true);
		break;

	default:
		rc = _detzone_ns_mgmt_submit_request(ch, bdev_io);
		break;;
	}

	if (rc == -EAGAIN) {
		//vbdev_detzone_cong_io_wait(bdev_io);
		//bdev_io->internal.error.aio_result = rc;
		//spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_AIO_ERROR);
	} else if (rc == -ENOMEM) {
		SPDK_ERRLOG("No memory, start to queue io for detzone.\n");
		vbdev_detzone_queue_io(bdev_io);
	} else if (rc != 0) {
		SPDK_ERRLOG("ERROR on bdev_io submission!\n");
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static bool
vbdev_detzone_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)ctx;

	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_ZCOPY:
	case SPDK_BDEV_IO_TYPE_NVME_ADMIN:
	case SPDK_BDEV_IO_TYPE_NVME_IO:
	case SPDK_BDEV_IO_TYPE_NVME_IO_MD:
	case SPDK_BDEV_IO_TYPE_COMPARE:
	case SPDK_BDEV_IO_TYPE_COMPARE_AND_WRITE:
		return false;
	default:
		return spdk_bdev_io_type_supported(detzone_ctrlr->base_bdev, io_type);
	}
}

static bool
vbdev_detzone_ns_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	struct vbdev_detzone_ns *detzone_ns = (struct vbdev_detzone_ns *)ctx;

	return vbdev_detzone_io_type_supported(detzone_ns->ctrl, io_type);
}

static struct spdk_io_channel *
vbdev_detzone_get_io_channel(void *ctx)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)ctx;

	return spdk_get_io_channel(detzone_ctrlr);
}

static struct spdk_io_channel *
vbdev_detzone_ns_get_io_channel(void *ctx)
{
	struct vbdev_detzone *detzone_ns = (struct vbdev_detzone *)ctx;

	return spdk_get_io_channel(detzone_ns);
}

static int
vbdev_detzone_ns_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	//struct vbdev_detzone_ns *detzone_ns = (struct vbdev_detzone_ns *)ctx;
	/*
	spdk_json_write_name(w, "detzone_ns");
	spdk_json_write_object_begin(w);

	spdk_json_write_named_string(w, "ns_name", spdk_bdev_get_name(&detzone_ns->detzone_ns_bdev));
	spdk_json_write_named_uint64(w, "stripe_size", );

	spdk_json_write_object_end(w);
	*/
	return 0;
}

static void
_detzone_ns_write_conf_values(struct vbdev_detzone_ns *detzone_ns, struct spdk_json_write_ctx *w)
{
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;

	spdk_json_write_named_string(w, "ns_name", spdk_bdev_get_name(&detzone_ns->detzone_ns_bdev));
	spdk_json_write_named_string(w, "ctrl_name", spdk_bdev_get_name(&detzone_ctrlr->mgmt_bdev));
	spdk_json_write_named_uint32(w, "zone_array_size", detzone_ns->zone_array_size);
	spdk_json_write_named_uint32(w, "stripe_size", detzone_ns->stripe_blocks);
	spdk_json_write_named_uint32(w, "block_align", detzone_ns->block_align);
}

static void
_detzone_write_conf_values(struct vbdev_detzone *detzone_ctrlr, struct spdk_json_write_ctx *w)
{
	spdk_json_write_named_string(w, "name", spdk_bdev_get_name(&detzone_ctrlr->mgmt_bdev));
	spdk_json_write_named_string(w, "base_bdev_name", spdk_bdev_get_name(detzone_ctrlr->base_bdev));
	spdk_json_write_named_uint32(w, "num_pu", detzone_ctrlr->num_pu);
}

static int
vbdev_detzone_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)ctx;
	struct vbdev_detzone_ns *detzone_ns;

	spdk_json_write_name(w, "detzone");
	spdk_json_write_object_begin(w);
	_detzone_write_conf_values(detzone_ctrlr, w);

	spdk_json_write_named_array_begin(w, "namespaces");
	TAILQ_FOREACH(detzone_ns, &detzone_ctrlr->ns, link) {
		spdk_json_write_string(w, spdk_bdev_get_name(&detzone_ns->detzone_ns_bdev));
	}

	spdk_json_write_array_end(w);
	spdk_json_write_object_end(w);

	return 0;
}

/* This is used to generate JSON that can configure this module to its current state. */
static int
vbdev_detzone_config_json(struct spdk_json_write_ctx *w)
{
	struct vbdev_detzone *detzone_ctrlr;
	struct vbdev_detzone_ns *detzone_ns;

	TAILQ_FOREACH(detzone_ctrlr, &g_detzone_ctrlrs, link) {
		spdk_json_write_object_begin(w);
		spdk_json_write_named_string(w, "method", "bdev_detzone_create");
		spdk_json_write_named_object_begin(w, "params");
		_detzone_write_conf_values(detzone_ctrlr, w);
		spdk_json_write_object_end(w);
		spdk_json_write_object_end(w);

		TAILQ_FOREACH(detzone_ns, &detzone_ctrlr->ns, link) {
			spdk_json_write_object_begin(w);
			spdk_json_write_named_string(w, "method", "bdev_detzone_ns_create");
			spdk_json_write_named_object_begin(w, "params");
			_detzone_ns_write_conf_values(detzone_ns, w);
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
detzone_bdev_io_ch_create_cb(void *io_device, void *ctx_buf)
{
	struct detzone_io_channel *detzone_ch = ctx_buf;
	struct vbdev_detzone_ns *detzone_ns = io_device;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;

	// currently, allow only single thread for the namespace
	//if (detzone_ns->thread && detzone_ns->thread != spdk_get_thread()) {
	//	return -EINVAL;
	//}

	detzone_ch->base_ch = spdk_bdev_get_io_channel(detzone_ctrlr->base_desc);
	detzone_ch->ch_id = ++detzone_ns->ref;
	//detzone_ch->rd_avail_window = VBDEV_DETZONE_SLIDEWIN_MAX / detzone_ns->detzone_ns_bdev.blocklen;
	detzone_ch->rd_avail_window = detzone_ns->stripe_blocks * detzone_ns->zone_array_size;
	//printf("rd_slidwin_size: %lu\n", detzone_ch->rd_avail_window);
	//detzone_ch->rd_avail_window = 8;
	detzone_ch->wr_avail_window = detzone_ns->stripe_blocks * detzone_ns->zone_array_size;

	TAILQ_INIT(&detzone_ch->rd_slidewin_queue);
	TAILQ_INIT(&detzone_ch->wr_slidewin_queue);
	TAILQ_INIT(&detzone_ch->write_drr_queue);
	return 0;
}

static int
detzone_bdev_mgmt_ch_create_cb(void *io_device, void *ctx_buf)
{
	struct detzone_mgmt_channel *detzone_mgmt_ch = ctx_buf;
	struct vbdev_detzone *detzone_ctrlr = io_device;

	//detzone_mgmt_ch->io_poller = SPDK_POLLER_REGISTER(TBD, detzone_mgmt_ch, 100);
	detzone_ctrlr->mgmt_ch = spdk_bdev_get_io_channel(detzone_ctrlr->base_desc);

	return 0;
}

/* We provide this callback for the SPDK channel code to destroy a channel
 * created with our create callback. We just need to undo anything we did
 * when we created. If this bdev used its own poller, we'd unregsiter it here.
 */
static void
detzone_bdev_io_ch_destroy_cb(void *io_device, void *ctx_buf)
{
	struct detzone_io_channel *detzone_ch = ctx_buf;

	spdk_put_io_channel(detzone_ch->base_ch);
}

static void
detzone_bdev_mgmt_ch_destroy_cb(void *io_device, void *ctx_buf)
{
	struct detzone_mgmt_channel *detzone_mgmt_ch = ctx_buf;
	struct vbdev_detzone *detzone_ctrlr = io_device;

	spdk_poller_unregister(&detzone_mgmt_ch->io_poller);
	spdk_put_io_channel(detzone_ctrlr->mgmt_ch);
}

/* Create the detzone association from the bdev and vbdev name and insert
 * on the global list. */
static int
vbdev_detzone_ns_insert_association(const char *detzone_name, const char *ns_name,
					uint32_t zone_array_size, uint32_t stripe_size, uint32_t block_align,
					uint64_t start_base_zone, uint64_t num_base_zones)
{
	struct bdev_association *bdev_assoc;
	struct ns_association *assoc;

	TAILQ_FOREACH(bdev_assoc, &g_bdev_associations, link) {
		if (strcmp(detzone_name, bdev_assoc->vbdev_name)) {
			continue;
		}

		TAILQ_FOREACH(assoc, &g_ns_associations, link) {
			if (strcmp(ns_name, assoc->ns_name) == 0 && strcmp(detzone_name, assoc->ctrl_name) == 0) {
				SPDK_ERRLOG("detzone ns bdev %s/%s already exists\n", detzone_name, ns_name);
				return -EEXIST;
			}
		}

		assoc = calloc(1, sizeof(struct ns_association));
		if (!assoc) {
			SPDK_ERRLOG("could not allocate bdev_association\n");
			return -ENOMEM;
		}

		assoc->ctrl_name = strdup(detzone_name);
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
		assoc->start_base_zone = start_base_zone;
		assoc->num_base_zones = num_base_zones;
		TAILQ_INSERT_TAIL(&g_ns_associations, assoc, link);

		return 0;
	}

	SPDK_ERRLOG("Unable to insert ns %s assoc because the detzone bdev %s doesn't exist.\n", ns_name, detzone_name);
	return -ENODEV;
}

/* Create the detzone association from the bdev and vbdev name and insert
 * on the global list. */
static int
vbdev_detzone_insert_association(const char *bdev_name, const char *vbdev_name,
				   uint32_t num_pu)
{
	struct bdev_association *assoc;

	TAILQ_FOREACH(assoc, &g_bdev_associations, link) {
		if (strcmp(vbdev_name, assoc->vbdev_name) == 0) {
			SPDK_ERRLOG("detzone bdev %s already exists\n", vbdev_name);
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

	assoc->num_pu = num_pu;

	TAILQ_INSERT_TAIL(&g_bdev_associations, assoc, link);

	return 0;
}

static int
vbdev_detzone_init(void)
{
	/* Not allowing for .ini style configuration. */
	return 0;
}

static void
vbdev_detzone_finish(void)
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
vbdev_detzone_get_ctx_size(void)
{
	return sizeof(struct detzone_bdev_io);
}

static int
vbdev_detzone_get_memory_domains(void *ctx, struct spdk_memory_domain **domains, int array_size)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)ctx;

	/* detzone bdev doesn't work with data buffers, so it supports any memory domain used by base_bdev */
	return spdk_bdev_get_memory_domains(detzone_ctrlr->base_bdev, domains, array_size);
}

/* Callback for unregistering the IO device. */
static void
_ns_unregister_cb(void *io_device)
{
	struct vbdev_detzone_ns *detzone_ns  = io_device;

	/* Done with this detzone_ns. */
	free(detzone_ns->detzone_ns_bdev.name);
	free(detzone_ns);
}

static int
vbdev_detzone_ns_destruct(void *ctx)
{
	struct vbdev_detzone_ns *detzone_ns = (struct vbdev_detzone_ns *)ctx;
	struct vbdev_detzone    *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	/* It is important to follow this exact sequence of steps for destroying
	 * a vbdev...
	 */

	TAILQ_REMOVE(&detzone_ctrlr->ns, detzone_ns, link);

	/* Unregister the io_device. */
	spdk_io_device_unregister(detzone_ns, _ns_unregister_cb);

	return 0;
}

/* When we register our bdev this is how we specify our entry points. */
static const struct spdk_bdev_fn_table vbdev_detzone_ns_fn_table = {
	.destruct		= vbdev_detzone_ns_destruct,
	.submit_request		= vbdev_detzone_ns_submit_request,
	.io_type_supported	= vbdev_detzone_ns_io_type_supported,
	.get_io_channel		= vbdev_detzone_ns_get_io_channel,
	.dump_info_json		= vbdev_detzone_ns_dump_info_json,
	.write_config_json	= NULL,
	.get_memory_domains	= vbdev_detzone_get_memory_domains,
};

static int
vbdev_detzone_ns_register(const char *ctrl_name, const char *ns_name)
{
	struct ns_association *assoc;
	struct vbdev_detzone *detzone_ctrlr;
	struct vbdev_detzone_ns *detzone_ns = NULL;
	struct vbdev_detzone_ns_zone_info *zone_info;
	struct vbdev_detzone_zone_info *phy_zone_info;
	struct spdk_bdev *bdev;
	uint64_t total_lzones, i, zone_idx;
	uint32_t j;
	int rc = 0;

	TAILQ_FOREACH(assoc, &g_ns_associations, link) {
		if (strcmp(assoc->ctrl_name, ctrl_name)) {
			continue;
		} else if (ns_name && strcmp(assoc->ns_name, ns_name)) {
			continue;
		}

		TAILQ_FOREACH(detzone_ctrlr, &g_detzone_ctrlrs, link) {
			if (strcmp(detzone_ctrlr->mgmt_bdev.name, ctrl_name)) {
				continue;
			}

			TAILQ_FOREACH(detzone_ns, &detzone_ctrlr->ns, link) {
				if (!strcmp(detzone_ns->detzone_ns_bdev.name, assoc->ns_name)) {
					SPDK_ERRLOG("the ns name %s already exists on detzone_ctrlr %s\n", assoc->ns_name, ctrl_name);
					return -EEXIST;
				}
			}

			total_lzones = assoc->zone_array_size ? assoc->num_base_zones / assoc->zone_array_size : assoc->num_base_zones;
			if (total_lzones == 0) {
				SPDK_ERRLOG("could not create zero sized detzone namespace\n");
				return -EINVAL;
			}
			detzone_ns = calloc(1, sizeof(struct vbdev_detzone_ns) +
							 (sizeof(struct vbdev_detzone_ns_zone_info) +
								 sizeof(uint64_t) * assoc->zone_array_size)
							 * total_lzones);
			if (!detzone_ns) {
				SPDK_ERRLOG("could not allocate detzone_ctrlr\n");
				return -ENOMEM;
			}
			detzone_ns->detzone_ns_bdev.name = strdup(assoc->ns_name);
			if (!detzone_ns->detzone_ns_bdev.name) {
				SPDK_ERRLOG("could not allocate detzone_bdev name\n");
				free(detzone_ns);
				return -ENOMEM;
			}
			detzone_ns->detzone_ns_bdev.product_name = "detzone";

			detzone_ns->ctrl = detzone_ctrlr;
			detzone_ns->nsid = detzone_ctrlr->num_ns + 1;
			bdev = detzone_ctrlr->base_bdev;

			detzone_ns->detzone_ns_bdev.ctxt = detzone_ns;
			detzone_ns->detzone_ns_bdev.fn_table = &vbdev_detzone_ns_fn_table;
			detzone_ns->detzone_ns_bdev.module = &detzone_if;

			detzone_ns->detzone_ns_bdev.zoned = true;

			if (assoc->stripe_size && (assoc->stripe_size % bdev->blocklen)) { 
				rc = -EINVAL;
				SPDK_ERRLOG("stripe size must be block size aligned\n");
				goto error_close;
			}
			detzone_ns->stripe_blocks = assoc->stripe_size ? (assoc->stripe_size / bdev->blocklen) : bdev->optimal_io_boundary;
			if (detzone_ns->stripe_blocks == 0) {
				detzone_ns->stripe_blocks = 1;
			}
			if (bdev->zone_size % detzone_ns->stripe_blocks) {
				rc = -EINVAL;
				SPDK_ERRLOG("base bdev zone size must be stripe size aligned\n");
				goto error_close;
			}
			detzone_ns->block_align = assoc->block_align;

			detzone_ns->detzone_ns_bdev.write_cache = bdev->write_cache;
			detzone_ns->detzone_ns_bdev.optimal_io_boundary = bdev->optimal_io_boundary;

			detzone_ns->base_zone_size = bdev->zone_size;
			// Configure namespace specific parameters
			detzone_ns->zone_array_size = assoc->zone_array_size ? assoc->zone_array_size : 1;
			detzone_ns->num_base_zones = assoc->num_base_zones;

			//TODO: should check base_bdev zone capacity
			detzone_ns->zcap = detzone_ns->base_zone_size * detzone_ns->zone_array_size;
			//detzone_ns->detzone_ns_bdev.zone_size = spdk_align64pow2(detzone_ns->zcap);
			detzone_ns->detzone_ns_bdev.zone_size = detzone_ns->zcap;
			detzone_ns->detzone_ns_bdev.required_alignment = bdev->required_alignment;
			detzone_ns->detzone_ns_bdev.max_zone_append_size = bdev->max_zone_append_size;
			detzone_ns->detzone_ns_bdev.max_open_zones = 1;
			detzone_ns->detzone_ns_bdev.max_active_zones = 1;
			detzone_ns->detzone_ns_bdev.optimal_open_zones = 1;
		
			detzone_ns->detzone_ns_bdev.blocklen = bdev->blocklen;
			// TODO: support configurable block length (blocklen)
			//detzone_ns->detzone_ns_bdev.phys_blocklen = bdev->blocklen;

			detzone_ns->detzone_ns_bdev.blockcnt = total_lzones * detzone_ns->detzone_ns_bdev.zone_size;

			// it looks dumb... but let just keep it...
			zone_info = detzone_ns->zone_info;
			phy_zone_info = detzone_ctrlr->zone_info;
			for (i=0; i < total_lzones; i++) {
				for (j=0; j < detzone_ns->zone_array_size; j++) {
					zone_info[i].base_zone_id[j] = UINT64_MAX;
				}
				zone_info[i].write_pointer = detzone_ns->detzone_ns_bdev.zone_size * i;
				zone_info[i].capacity = detzone_ns->zcap;
				zone_info[i].state = SPDK_BDEV_ZONE_STATE_EMPTY;
				zone_info[i].pu_group = detzone_ctrlr->num_pu;
			}
			for (i=0; i < detzone_ctrlr->num_zones; i++) {
				if (phy_zone_info[i].ns_id != detzone_ns->nsid) {
					continue;
				}
				if (phy_zone_info[i].stripe_width != detzone_ns->zone_array_size ||
						phy_zone_info[i].stripe_size != detzone_ns->stripe_blocks) {
					SPDK_ERRLOG("Stripe metadata does not match\n");
					goto error_close;
				}
				zone_idx = phy_zone_info[i].lzone_id / detzone_ns->detzone_ns_bdev.zone_size;
				zone_info[zone_idx].base_zone_id[phy_zone_info[i].stripe_id] = phy_zone_info[i].zone_id;
				// TODO: if any zone has a partially written stripe, we have to recover.
				// we may copy valid data to another physical zones and discard the partial write. 
				zone_info[zone_idx].write_pointer += phy_zone_info[i].write_pointer - phy_zone_info[i].zone_id;
				zone_info[zone_idx].state = phy_zone_info[i].state;	
			}

			spdk_io_device_register(detzone_ns, detzone_bdev_io_ch_create_cb, detzone_bdev_io_ch_destroy_cb,
						sizeof(struct detzone_io_channel),
						assoc->ns_name);
			
			detzone_ns->thread = spdk_get_thread();

			rc = spdk_bdev_register(&detzone_ns->detzone_ns_bdev);
			if (rc) {
				SPDK_ERRLOG("could not register detzone_ns_bdev\n");
				goto error_close;
			}

			++detzone_ctrlr->num_ns;
			TAILQ_INSERT_TAIL(&detzone_ctrlr->ns, detzone_ns, link);
		}
	}

	return 0;

error_close:
	spdk_io_device_unregister(detzone_ns, NULL);
	free(detzone_ns->detzone_ns_bdev.name);
	free(detzone_ns);
	return rc;
}

static void
vbdev_detzone_base_bdev_hotremove_cb(struct spdk_bdev *bdev_find)
{
	struct vbdev_detzone *detzone_ctrlr, *tmp;

	TAILQ_FOREACH_SAFE(detzone_ctrlr, &g_detzone_ctrlrs, link, tmp) {
		if (bdev_find == detzone_ctrlr->base_bdev) {
			spdk_bdev_unregister(&detzone_ctrlr->mgmt_bdev, NULL, NULL);
		}
	}
}

static void
_device_unregister_cb(void *io_device)
{
	struct vbdev_detzone *detzone_ctrlr  = io_device;

	/* Done with this detzone_ctrlr. */
	free(detzone_ctrlr->zone_info);
	free(detzone_ctrlr->mgmt_bdev.name);
	free(detzone_ctrlr);
}

static void
_vbdev_detzone_destruct_cb(void *ctx)
{
	struct spdk_bdev_desc *desc = ctx;

	spdk_bdev_close(desc);
}

static int
vbdev_detzone_destruct(void *ctx)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)ctx;
	struct vbdev_detzone_ns *detzone_ns, *tmp_ns;

	/* It is important to follow this exact sequence of steps for destroying
	 * a vbdev...
	 */
	TAILQ_FOREACH_SAFE(detzone_ns, &detzone_ctrlr->ns, link, tmp_ns) {
		spdk_bdev_unregister(&detzone_ns->detzone_ns_bdev, NULL, NULL);
	}

	TAILQ_REMOVE(&g_detzone_ctrlrs, detzone_ctrlr, link);

	/* Unclaim the underlying bdev. */
	spdk_bdev_module_release_bdev(detzone_ctrlr->base_bdev);

	/* Close the underlying bdev on its same opened thread. */
	if (detzone_ctrlr->thread && detzone_ctrlr->thread != spdk_get_thread()) {
		spdk_thread_send_msg(detzone_ctrlr->thread, _vbdev_detzone_destruct_cb, detzone_ctrlr->base_desc);
	} else {
		spdk_bdev_close(detzone_ctrlr->base_desc);
	}

	/* Unregister the io_device. */
	spdk_io_device_unregister(detzone_ctrlr, _device_unregister_cb);

	return 0;
}

/* When we register our bdev this is how we specify our entry points. */
static const struct spdk_bdev_fn_table vbdev_detzone_fn_table = {
	.destruct		= vbdev_detzone_destruct,
	.submit_request		= vbdev_detzone_mgmt_submit_request,
	.io_type_supported	= vbdev_detzone_io_type_supported,
	.get_io_channel		= vbdev_detzone_get_io_channel,
	.dump_info_json		= vbdev_detzone_dump_info_json,
	.write_config_json	= NULL,
	.get_memory_domains	= vbdev_detzone_get_memory_domains,
};

/* Called when the underlying base bdev triggers asynchronous event such as bdev removal. */
static void
vbdev_detzone_base_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
			       void *event_ctx)
{
	switch (type) {
	case SPDK_BDEV_EVENT_REMOVE:
		vbdev_detzone_base_bdev_hotremove_cb(bdev);
		break;
	default:
		SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
		break;
	}
}

static void
_vbdev_detzone_fin_init(void *arg)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)arg;
	int rc;

	// TODO: we just use the current json config now.
	// but should read namespace metadata from persistent storage and compare with json.
	rc = vbdev_detzone_ns_register(detzone_ctrlr->mgmt_bdev.name, NULL);
	if (rc) {
		SPDK_ERRLOG("Unable to create ns on the detzone bdev %s.\n", detzone_ctrlr->mgmt_bdev.name);
		spdk_bdev_module_release_bdev(detzone_ctrlr->base_bdev);
		spdk_bdev_close(detzone_ctrlr->base_desc);
		spdk_io_device_unregister(detzone_ctrlr, NULL);
		free(detzone_ctrlr->mgmt_bdev.name);
		free(detzone_ctrlr);
		return;
	}

	TAILQ_INSERT_TAIL(&g_detzone_ctrlrs, detzone_ctrlr, link);
}

static void
_vbdev_detzone_init_zone_ext_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone *detzone_ctrlr = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone, mgmt_bdev);
	struct spdk_bdev_zone_ext_info *ext_info = cb_arg;
	struct vbdev_detzone_zone_md *md;
	uint64_t zone_idx;

	if (!success) {
		// indicate we cannot read zone desc extension and have to fail
		detzone_ctrlr->num_zones = 0;
	} else {
		zone_idx = ext_info->zone_id / detzone_ctrlr->base_bdev->zone_size;
		md = (struct vbdev_detzone_zone_md *)ext_info->ext;
		detzone_ctrlr->zone_info[zone_idx].ns_id = md->ns_id;
		detzone_ctrlr->zone_info[zone_idx].lzone_id = md->lzone_id;
		detzone_ctrlr->zone_info[zone_idx].stripe_id = md->stripe_id;
		detzone_ctrlr->zone_info[zone_idx].stripe_width = md->stripe_width;
		detzone_ctrlr->zone_info[zone_idx].stripe_size = md->stripe_size;
	}
	free(ext_info);

	if (--detzone_ctrlr->zone_alloc_cnt == 0) {
		if (detzone_ctrlr->num_zones == 0) {
			SPDK_ERRLOG("cannot retrieve the physical zone info for %s\n", detzone_ctrlr->mgmt_bdev.name);
			spdk_bdev_module_release_bdev(detzone_ctrlr->base_bdev);
			spdk_bdev_close(detzone_ctrlr->base_desc);
			spdk_io_device_unregister(detzone_ctrlr, NULL);
			free(detzone_ctrlr->zone_info);
			free(detzone_ctrlr->mgmt_bdev.name);
			free(detzone_ctrlr);
			return;
		}

		_vbdev_detzone_fin_init(detzone_ctrlr);
	}
}

static void
_vbdev_detzone_init_zone_info_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone *detzone_ctrlr = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone, mgmt_bdev);
	struct spdk_bdev_zone_info *info = (struct spdk_bdev_zone_info *)cb_arg;
	struct spdk_bdev_zone_ext_info *ext_info;
	uint64_t zone_idx;

	spdk_bdev_free_io(bdev_io);
	if (!success) {
		SPDK_ERRLOG("cannot retrieve the physical zone info for %s\n", detzone_ctrlr->mgmt_bdev.name);
		spdk_bdev_module_release_bdev(detzone_ctrlr->base_bdev);
		spdk_bdev_close(detzone_ctrlr->base_desc);
		spdk_io_device_unregister(detzone_ctrlr, NULL);
		free(detzone_ctrlr->zone_info);
		free(detzone_ctrlr->mgmt_bdev.name);
		free(detzone_ctrlr);
		return;
	}

	for (zone_idx = 0; zone_idx < detzone_ctrlr->num_zones; zone_idx++)
	{
		detzone_ctrlr->zone_info[zone_idx].state = info[zone_idx].state;
		detzone_ctrlr->zone_info[zone_idx].write_pointer = info[zone_idx].write_pointer;
		detzone_ctrlr->zone_info[zone_idx].zone_id = info[zone_idx].zone_id;
		detzone_ctrlr->zone_info[zone_idx].capacity = info[zone_idx].capacity;
		// We don't care what was the PU group of this zone
		// as the same algorithm results the same condition.
		detzone_ctrlr->zone_info[zone_idx].pu_group = 0;
		detzone_ctrlr->zone_info[zone_idx].ns_id = 0;

		if (info[zone_idx].state != SPDK_BDEV_ZONE_STATE_EMPTY) {
			ext_info = calloc(1, sizeof(struct spdk_bdev_zone_ext_info));
			
			// this means nothing, but use as temporal counter
			detzone_ctrlr->zone_alloc_cnt++;	
			spdk_bdev_get_zone_ext_info(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch, info[zone_idx].zone_id,
											1, ext_info, _vbdev_detzone_init_zone_ext_cb, ext_info);
		}
	}
	free(info);
	if (detzone_ctrlr->zone_alloc_cnt == 0) {
		// This device is fresh one. We are good to go.
		_vbdev_detzone_fin_init(detzone_ctrlr);
	}
}

/* Create and register the detzone vbdev if we find it in our list of bdev names.
 * This can be called either by the examine path or RPC method.
 */
static int
vbdev_detzone_register(const char *bdev_name)
{
	struct bdev_association *assoc;
	struct vbdev_detzone *detzone_ctrlr;
	struct spdk_bdev *bdev;
	int rc = 0;
	struct spdk_bdev_zone_info *info;

	/* Check our list of names from config versus this bdev and if
	 * there's a match, create the detzone_ctrlr & bdev accordingly.
	 */
	TAILQ_FOREACH(assoc, &g_bdev_associations, link) {
		if (strcmp(assoc->bdev_name, bdev_name) != 0) {
			continue;
		}

		detzone_ctrlr = calloc(1, sizeof(struct vbdev_detzone));
		if (!detzone_ctrlr) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate detzone_ctrlr\n");
			break;
		}
		TAILQ_INIT(&detzone_ctrlr->ns);
		TAILQ_INIT(&detzone_ctrlr->ns_active);
		TAILQ_INIT(&detzone_ctrlr->ns_pending);

		STAILQ_INIT(&detzone_ctrlr->zone_reserved);
		STAILQ_INIT(&detzone_ctrlr->zone_empty);

		// Create the detzone mgmt_ns
		detzone_ctrlr->mgmt_bdev.name = strdup(assoc->vbdev_name);
		if (!detzone_ctrlr->mgmt_bdev.name) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate detzone_bdev name\n");
			free(detzone_ctrlr);
			break;
		}
		detzone_ctrlr->mgmt_bdev.product_name = "detzone";

		/* The base bdev that we're attaching to. */
		rc = spdk_bdev_open_ext(bdev_name, true, vbdev_detzone_base_bdev_event_cb,
					NULL, &detzone_ctrlr->base_desc);

		if (rc) {
			if (rc != -ENODEV) {
				SPDK_ERRLOG("could not open bdev %s\n", bdev_name);
			}
			free(detzone_ctrlr->mgmt_bdev.name);
			free(detzone_ctrlr);
			break;
		}

		bdev = spdk_bdev_desc_get_bdev(detzone_ctrlr->base_desc);
		if (!spdk_bdev_is_zoned(bdev) || strcmp(spdk_bdev_get_module_name(bdev), "bdev_nvme")) {
			rc = -EINVAL;
			SPDK_ERRLOG("detzone does not support non-zoned or non-nvme devices\n");
			free(detzone_ctrlr->mgmt_bdev.name);
			free(detzone_ctrlr);
			break;
		}

		if (!spdk_bdev_get_num_zones(bdev)) {
			rc = -EINVAL;
			SPDK_ERRLOG("targer device has no zones\n");
			free(detzone_ctrlr->mgmt_bdev.name);
			free(detzone_ctrlr);
			break;
		}

		detzone_ctrlr->base_bdev = bdev;

		detzone_ctrlr->mgmt_bdev.write_cache = bdev->write_cache;
		detzone_ctrlr->mgmt_bdev.required_alignment = bdev->required_alignment;
		detzone_ctrlr->mgmt_bdev.optimal_io_boundary = bdev->optimal_io_boundary;
		detzone_ctrlr->mgmt_bdev.blocklen = bdev->blocklen;
		detzone_ctrlr->mgmt_bdev.blockcnt = bdev->blockcnt;

		detzone_ctrlr->mgmt_bdev.zoned = true;
		detzone_ctrlr->mgmt_bdev.zone_size = bdev->zone_size;
		detzone_ctrlr->mgmt_bdev.max_zone_append_size = bdev->max_zone_append_size;
		detzone_ctrlr->mgmt_bdev.max_open_zones = bdev->max_open_zones;
		detzone_ctrlr->mgmt_bdev.max_active_zones = bdev->max_active_zones;
		detzone_ctrlr->mgmt_bdev.optimal_open_zones = bdev->optimal_open_zones;

		detzone_ctrlr->mgmt_bdev.ctxt = detzone_ctrlr;
		detzone_ctrlr->mgmt_bdev.fn_table = &vbdev_detzone_fn_table;
		detzone_ctrlr->mgmt_bdev.module = &detzone_if;

		detzone_ctrlr->num_pu = assoc->num_pu;
		detzone_ctrlr->zone_alloc_cnt = 0;
		detzone_ctrlr->num_zones = spdk_bdev_get_num_zones(bdev);
		detzone_ctrlr->zone_info = calloc(detzone_ctrlr->num_zones, sizeof(struct vbdev_detzone_zone_info));

		detzone_ctrlr->num_ns = 0;
		/* set 0 to current claimed blockcnt */
		detzone_ctrlr->claimed_blockcnt = 0;

		/* Save the thread where the base device is opened */
		detzone_ctrlr->thread = spdk_get_thread();

		spdk_io_device_register(detzone_ctrlr, detzone_bdev_mgmt_ch_create_cb, detzone_bdev_mgmt_ch_destroy_cb,
					sizeof(struct detzone_io_channel),
					assoc->vbdev_name);

		/* claim the base_bdev only if this is the first detzone node */
		rc = spdk_bdev_module_claim_bdev(bdev, detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_bdev.module);
		if (rc) {
			SPDK_ERRLOG("could not claim bdev %s\n", bdev_name);
			goto error_close;
		}

		rc = spdk_bdev_register(&detzone_ctrlr->mgmt_bdev);
		if (rc) {
			SPDK_ERRLOG("could not register detzone mgmt bdev\n");
			spdk_bdev_module_release_bdev(bdev);
			goto error_close;
		}

		// Retrieve previous zone allocations.
		// We will register namespaces after that.
		info = calloc(detzone_ctrlr->num_zones, sizeof(struct spdk_bdev_zone_info));
		rc = spdk_bdev_get_zone_info(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
				     0, detzone_ctrlr->num_zones, info,
				     _vbdev_detzone_init_zone_info_cb, info);
		if (rc) {
			SPDK_ERRLOG("Unable to get init zone info of the detzone bdev %s.\n", detzone_ctrlr->mgmt_bdev.name);
			spdk_bdev_module_release_bdev(bdev);
			goto error_close;
		}
	}

	return rc;

error_close:
	spdk_bdev_close(detzone_ctrlr->base_desc);
	spdk_io_device_unregister(detzone_ctrlr, NULL);
	free(detzone_ctrlr->zone_info);
	free(detzone_ctrlr->mgmt_bdev.name);
	free(detzone_ctrlr);
	return rc;
}

int
create_detzone_ns(const char *detzone_name, const char *ns_name,
					uint32_t zone_array_size, uint32_t stripe_size, uint32_t block_align,
					uint64_t start_base_zone, uint64_t num_base_zones)
{
	struct ns_association *assoc;
	int rc = 0;

	rc = vbdev_detzone_ns_insert_association(detzone_name, ns_name,
										 zone_array_size, stripe_size, block_align,
										 start_base_zone, num_base_zones);
	if (rc) {
		return rc;
	}

	rc = vbdev_detzone_ns_register(detzone_name, ns_name);
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
delete_detzone_ns(struct spdk_bdev *bdev, spdk_bdev_unregister_cb cb_fn, void *cb_arg)
{
	struct ns_association *assoc;

	if (!bdev || bdev->module != &detzone_if) {
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
create_detzone_disk(const char *bdev_name, const char *vbdev_name, uint32_t num_pu)
{
	struct bdev_association *assoc;
	int rc = 0;

	rc = vbdev_detzone_insert_association(bdev_name, vbdev_name, num_pu);
	if (rc) {
		return rc;
	}

	rc = vbdev_detzone_register(bdev_name);
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
delete_detzone_disk(struct spdk_bdev *bdev, spdk_bdev_unregister_cb cb_fn, void *cb_arg)
{
	struct bdev_association *assoc;

	if (!bdev || bdev->module != &detzone_if) {
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
vbdev_detzone_examine(struct spdk_bdev *bdev)
{
	vbdev_detzone_register(bdev->name);

	spdk_bdev_module_examine_done(&detzone_if);
}

SPDK_LOG_REGISTER_COMPONENT(vbdev_detzone)

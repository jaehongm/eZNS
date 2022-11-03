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
#include "spdk/bit_array.h"

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
	.examine_disk = vbdev_detzone_examine,
	.module_fini = vbdev_detzone_finish,
	.config_json = vbdev_detzone_config_json
};

SPDK_BDEV_MODULE_REGISTER(detzone, &detzone_if)

/* Shared memory pool for management commands */
static struct spdk_mempool *g_detzone_mgmt_buf_pool;

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
	uint32_t	zone_stripe_width;
	uint32_t	stripe_size;
	uint32_t	block_align;

	uint64_t	num_base_zones;

	TAILQ_ENTRY(ns_association)	link;
};
static TAILQ_HEAD(, ns_association) g_ns_associations = TAILQ_HEAD_INITIALIZER(
			g_ns_associations);


static TAILQ_HEAD(, vbdev_detzone) g_detzone_ctrlrs = TAILQ_HEAD_INITIALIZER(g_detzone_ctrlrs);

struct vbdev_detzone_register_ctx {
	struct vbdev_detzone 			*detzone_ctrlr;
	struct spdk_bdev_zone_ext_info 	*ext_info;
	struct vbdev_detzone_zone_md 	*zone_md;

	vbdev_detzone_register_cb cb;
	void *cb_arg;
};

static void vbdev_detzone_ns_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io);
static void vbdev_detzone_reserve_zone(void *arg);
static void vbdev_detzone_ns_dealloc_zone(struct vbdev_detzone_ns *detzone_ns, uint64_t zone_idx);
static void _vbdev_detzone_md_read_submit(struct vbdev_detzone_md_io_ctx *md_io_ctx);

static inline uint64_t
vbdev_detzone_ns_get_zone_idx(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	// TODO: use shift operator
	return slba / detzone_ns->detzone_ns_bdev.zone_size;
}

static inline uint64_t
vbdev_detzone_ns_get_zone_id(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	return slba - (slba % detzone_ns->detzone_ns_bdev.zone_size);
}

static inline uint64_t
vbdev_detzone_ns_get_zone_id_by_idx(struct vbdev_detzone_ns *detzone_ns, uint64_t idx)
{
	return detzone_ns->internal.zone[idx].zone_id;
}

static inline enum spdk_bdev_zone_state
vbdev_detzone_ns_get_zone_state(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	uint64_t zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, slba);

	return detzone_ns->internal.zone[zone_idx].state;
}

static inline uint64_t
vbdev_detzone_ns_get_zone_append_pointer(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	struct spdk_bdev_io *bdev_io;
	struct detzone_bdev_io *io_ctx;
	uint64_t zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, slba);
	uint64_t pending_write_blks = 0;

	TAILQ_FOREACH(io_ctx, &detzone_ns->internal.zone[zone_idx].wr_waiting_cpl, link) {
		bdev_io = spdk_bdev_io_from_ctx(io_ctx);
		pending_write_blks += bdev_io->u.bdev.num_blocks;
	}
	TAILQ_FOREACH(io_ctx, &detzone_ns->internal.zone[zone_idx].wr_pending_queue, link) {
		bdev_io = spdk_bdev_io_from_ctx(io_ctx);
		pending_write_blks += bdev_io->u.bdev.num_blocks;
	}
	return detzone_ns->internal.zone[zone_idx].write_pointer + pending_write_blks;
}

static inline uint64_t
vbdev_detzone_ns_get_zone_wp(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	uint64_t zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, slba);

	return detzone_ns->internal.zone[zone_idx].write_pointer;
}

static inline uint64_t
vbdev_detzone_ns_get_zone_cap(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	uint64_t zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, slba);

	return detzone_ns->internal.zone[zone_idx].capacity;
}

static inline void
vbdev_detzone_ns_zone_wp_forward(struct vbdev_detzone_ns *detzone_ns, uint64_t offset_blocks, uint64_t numblocks)
{
	//SPDK_DEBUGLOG(vbdev_detzone, "forward logi zone_wp : id(%lu) wp(%lu) forward(%lu)\n",
	//				detzone_ns->internal.zone[vbdev_detzone_ns_get_zone_idx(detzone_ns, offset_blocks)].zone_id,
	//				detzone_ns->internal.zone[vbdev_detzone_ns_get_zone_idx(detzone_ns, offset_blocks)].write_pointer,
	//				numblocks);
	detzone_ns->internal.zone[vbdev_detzone_ns_get_zone_idx(detzone_ns, offset_blocks)].write_pointer += numblocks;
}

static inline uint64_t
_vbdev_detzone_ns_get_basezone_idx(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	// TODO: use shift operator
	return (((slba % detzone_ns->detzone_ns_bdev.zone_size) / detzone_ns->zone_stripe_blks)) % detzone_ns->zone_stripe_width;
}

static inline uint64_t
_vbdev_detzone_ns_get_stripe_offset(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	// TODO: use shift operator
	return ((slba % detzone_ns->detzone_ns_bdev.zone_size) / (detzone_ns->zone_stripe_width * detzone_ns->zone_stripe_blks)) * detzone_ns->zone_stripe_blks;
}

static inline uint64_t
_vbdev_detzone_ns_get_phy_offset(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	// TODO: use shift operator
	uint64_t zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, slba);
	uint64_t stripe_idx = _vbdev_detzone_ns_get_basezone_idx(detzone_ns, slba);
	uint64_t stripe_offset = _vbdev_detzone_ns_get_stripe_offset(detzone_ns, slba);

	//SPDK_DEBUGLOG(vbdev_detzone, "slba: %lu zone_idx: %lu  stripe_idx: %lu  stripe_offset: %lu basezone_id: 0x%lx \n",
	//					 slba, zone_idx, stripe_idx, stripe_offset, detzone_ns->internal.zone[zone_idx].base_zone_id[stripe_idx]);
	if (detzone_ns->internal.zone[zone_idx].base_zone[stripe_idx].zone_id == UINT64_MAX) {
		return UINT64_MAX;
	} else {
		return detzone_ns->padding_blocks + // This is offset for the zone metadata (kind of workaround for the current F/W)
											   // first block is written at the reservation time, second is at the allocation time
				detzone_ns->internal.zone[zone_idx].base_zone[stripe_idx].zone_id + stripe_offset + (slba % detzone_ns->zone_stripe_blks);
	}
}

static inline uint64_t
_vbdev_detzone_ns_get_phy_zone_wp(struct vbdev_detzone_ns *detzone_ns, uint64_t phy_offset)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	uint64_t zone_idx = phy_offset / detzone_ctrlr->mgmt_bdev.zone_size;

	return detzone_ctrlr->zone_info[zone_idx].write_pointer;
}

static inline void
_vbdev_detzone_ns_forward_phy_zone_wp(struct vbdev_detzone_ns *detzone_ns, uint64_t phy_offset, uint64_t numblocks)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	uint64_t lzone_idx, zone_idx = phy_offset / detzone_ctrlr->mgmt_bdev.zone_size;
	uint64_t written_blks;
	uint32_t i;

	if (spdk_unlikely(phy_offset != detzone_ctrlr->zone_info[zone_idx].write_pointer)) {
		SPDK_DEBUGLOG(vbdev_detzone, "incorrect forwarding phy zone_wp : offset(%lu) id(%lu) wp(%lu) forward(%lu)\n",
					phy_offset,
					detzone_ctrlr->zone_info[zone_idx].zone_id,
					detzone_ctrlr->zone_info[zone_idx].write_pointer,
					numblocks);
		assert(phy_offset == detzone_ctrlr->zone_info[zone_idx].write_pointer);
	}
	detzone_ctrlr->zone_info[zone_idx].write_pointer += numblocks;
	written_blks = detzone_ctrlr->zone_info[zone_idx].write_pointer - detzone_ctrlr->zone_info[zone_idx].zone_id;
	assert(written_blks <= detzone_ctrlr->zone_info[zone_idx].capacity);
	if (written_blks == detzone_ctrlr->zone_info[zone_idx].capacity) {
		detzone_ctrlr->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_FULL;
		lzone_idx = detzone_ctrlr->zone_info[zone_idx].lzone_id / detzone_ns->detzone_ns_bdev.zone_size;
		for (i = 0; i < detzone_ns->zone_stripe_width; i++) {
			zone_idx = detzone_ns->internal.zone[lzone_idx].base_zone[i].zone_id / detzone_ctrlr->mgmt_bdev.zone_size;
			if (detzone_ctrlr->zone_info[zone_idx].state != SPDK_BDEV_ZONE_STATE_FULL) {
				return;
			}
		}
		//detzone_ns->internal.zone[lzone_idx].write_pointer = detzone_ns->internal.zone[lzone_idx].zone_id;
		detzone_ns->internal.zone[lzone_idx].state = SPDK_BDEV_ZONE_STATE_FULL;
		SPDK_DEBUGLOG(vbdev_detzone, "ZONE_FULL (0x%lx): release open(%u) active(%u)\n",
								 detzone_ctrlr->zone_info[zone_idx].lzone_id,
								 detzone_ns->num_open_zones,
								 detzone_ns->num_active_zones);
		detzone_ns->num_open_zones--;
		detzone_ns->num_active_zones--;
	}
}

static inline int
_vbdev_detzone_ns_append_zone_iov(struct vbdev_detzone_ns *detzone_ns, uint64_t slba,
										 void *buf, uint64_t blockcnt)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	struct vbdev_detzone_ns_zone *zone = detzone_ns->internal.zone;
	uint64_t basezone_idx = _vbdev_detzone_ns_get_basezone_idx(detzone_ns, slba);
	uint64_t zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, slba);
	int iov_idx;

	if (zone[zone_idx].wr_zone_in_progress
			|| zone[zone_idx].base_zone[basezone_idx].iov_cnt == DETZONE_WRITEV_MAX_IOVS
			|| zone[zone_idx].base_zone[basezone_idx].iov_blks + blockcnt >= detzone_ns->zone_stripe_tb_size) {
		return -EAGAIN;
	}
	
	iov_idx = zone[zone_idx].base_zone[basezone_idx].iov_cnt;
	zone[zone_idx].base_zone[basezone_idx].iovs[iov_idx].iov_base = buf;
	zone[zone_idx].base_zone[basezone_idx].iovs[iov_idx].iov_len = blockcnt * detzone_ns->detzone_ns_bdev.blocklen;
	zone[zone_idx].base_zone[basezone_idx].iov_blks += blockcnt;
	zone[zone_idx].base_zone[basezone_idx].iov_cnt += 1;
	return 0;
}

static void
_detzone_zone_management_complete(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = arg;

	mgmt_io_ctx->cb(mgmt_io_ctx->parent_io,
						 mgmt_io_ctx->nvme_status.sct, mgmt_io_ctx->nvme_status.sc,
						 mgmt_io_ctx->cb_arg);
	spdk_dma_free(mgmt_io_ctx->zone_mgmt.buf);
	free(mgmt_io_ctx);
}

static inline bool
_detzone_is_active_state(int state)
{
	switch (state) {
	case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
	case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
	case SPDK_BDEV_ZONE_STATE_CLOSED:
		return true;
	default:
		return false;
	}
}

static inline bool
_detzone_is_open_state(int state)
{
	switch (state) {
	case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
	case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		return true;
	default:
		return false;
	}
}

static void
_detzone_zone_management_done(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = cb_arg;
	struct vbdev_detzone *detzone_ctrlr = mgmt_io_ctx->detzone_ns->ctrl;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	uint64_t zone_idx, i;

	if (!success) {
		mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
		spdk_bdev_io_get_nvme_status(bdev_io, &mgmt_io_ctx->nvme_status.cdw0,
							 &mgmt_io_ctx->nvme_status.sct, &mgmt_io_ctx->nvme_status.sc);
	} else {
		zone_idx = bdev_io->u.zone_mgmt.zone_id / detzone_ctrlr->mgmt_bdev.zone_size;
		switch (bdev_io->u.zone_mgmt.zone_action) {
		case SPDK_BDEV_ZONE_RESET:
			detzone_ctrlr->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_EMPTY;
			break;
		case SPDK_BDEV_ZONE_CLOSE:
			detzone_ctrlr->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_CLOSED;
			break;
		case SPDK_BDEV_ZONE_FINISH:
			detzone_ctrlr->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_FULL;
			break;
		case SPDK_BDEV_ZONE_OPEN:
			detzone_ctrlr->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_EXP_OPEN;
			break;
		case SPDK_BDEV_ZONE_OFFLINE:
			detzone_ctrlr->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_OFFLINE;
			break;
		default:
			assert(0);
		}
		
	}

	assert(mgmt_io_ctx->outstanding_mgmt_ios);
	mgmt_io_ctx->outstanding_mgmt_ios--;

	if (mgmt_io_ctx->outstanding_mgmt_ios == 0 && mgmt_io_ctx->remain_ios == 0) {
		zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
		if (mgmt_io_ctx->status == SPDK_BDEV_IO_STATUS_SUCCESS) {
			for (i = 0; i < mgmt_io_ctx->zone_mgmt.num_zones; i++) {
				SPDK_DEBUGLOG(vbdev_detzone, "Zone transition: %u\n", detzone_ns->internal.zone[zone_idx].state);
				switch (mgmt_io_ctx->zone_mgmt.zone_action) {
				case SPDK_BDEV_ZONE_CLOSE:
					if (_detzone_is_open_state(detzone_ns->internal.zone[zone_idx].state)) {
						SPDK_DEBUGLOG(vbdev_detzone, "CLOSE: release open(%u)\n", detzone_ns->num_open_zones);
						detzone_ns->num_open_zones--;
					}
					if (!_detzone_is_active_state(detzone_ns->internal.zone[zone_idx].state)) {
						SPDK_DEBUGLOG(vbdev_detzone, "CLOSE: release active(%u)\n", detzone_ns->num_active_zones);
						detzone_ns->num_active_zones++;
					}
					detzone_ns->internal.zone[zone_idx + i].state = SPDK_BDEV_ZONE_STATE_CLOSED;
					break;
				case SPDK_BDEV_ZONE_FINISH:
					if (_detzone_is_open_state(detzone_ns->internal.zone[zone_idx].state)) {
						SPDK_DEBUGLOG(vbdev_detzone, "FINISH: release open(%u)\n", detzone_ns->num_open_zones);
						detzone_ns->num_open_zones--;
					}
					if (_detzone_is_active_state(detzone_ns->internal.zone[zone_idx].state)) {
						SPDK_DEBUGLOG(vbdev_detzone, "FINISH: release active(%u)\n", detzone_ns->num_active_zones);
						detzone_ns->num_active_zones--;
					}
					detzone_ns->internal.zone[zone_idx + i].state = SPDK_BDEV_ZONE_STATE_FULL;
					//detzone_ns->internal.zone[zone_idx + i].write_pointer = 
					//									detzone_ns->internal.zone[zone_idx + i].zone_id;
					break;
				case SPDK_BDEV_ZONE_OPEN:
					if (!_detzone_is_open_state(detzone_ns->internal.zone[zone_idx].state)) {
						SPDK_DEBUGLOG(vbdev_detzone, "OPEN: acquire open(%u)\n", detzone_ns->num_open_zones);
						detzone_ns->num_open_zones++;
					}
					if (!_detzone_is_active_state(detzone_ns->internal.zone[zone_idx].state)) {
						SPDK_DEBUGLOG(vbdev_detzone, "OPEN: acquire active(%u)\n", detzone_ns->num_active_zones);
						detzone_ns->num_active_zones++;
					}
					detzone_ns->internal.zone[zone_idx + i].state = SPDK_BDEV_ZONE_STATE_EXP_OPEN;
					break;
				case SPDK_BDEV_ZONE_RESET:
					if (_detzone_is_open_state(detzone_ns->internal.zone[zone_idx].state)) {
						SPDK_DEBUGLOG(vbdev_detzone, "RESET: release open(%u)\n", detzone_ns->num_open_zones);
						detzone_ns->num_open_zones--;
					}
					if (_detzone_is_active_state(detzone_ns->internal.zone[zone_idx].state)) {
						SPDK_DEBUGLOG(vbdev_detzone, "RESET: release active(%u)\n", detzone_ns->num_active_zones);
						detzone_ns->num_active_zones--;
					}
					detzone_ns->internal.zone[zone_idx + i].state = SPDK_BDEV_ZONE_STATE_EMPTY;
					detzone_ns->internal.zone[zone_idx + i].write_pointer =
														 detzone_ns->internal.zone[zone_idx + i].zone_id;
					vbdev_detzone_ns_dealloc_zone(detzone_ns, zone_idx + i);
					break;
				case SPDK_BDEV_ZONE_OFFLINE:
					if (_detzone_is_open_state(detzone_ns->internal.zone[zone_idx].state)) {
						SPDK_DEBUGLOG(vbdev_detzone, "OFFLINE: acquire open(%u)\n", detzone_ns->num_open_zones);
						detzone_ns->num_open_zones--;
					}
					if (_detzone_is_active_state(detzone_ns->internal.zone[zone_idx].state)) {
						SPDK_DEBUGLOG(vbdev_detzone, "OFFLINE: release active(%u)\n", detzone_ns->num_active_zones);
						detzone_ns->num_active_zones--;
					}
					detzone_ns->internal.zone[zone_idx + i].state = SPDK_BDEV_ZONE_STATE_OFFLINE;
					detzone_ns->internal.zone[zone_idx + i].write_pointer =
														 detzone_ns->internal.zone[zone_idx + i].zone_id;
					vbdev_detzone_ns_dealloc_zone(detzone_ns, zone_idx + i);
					break;
				default:
					assert(0);
				}
			}
		} else {
			// TODO: No recovery now. We may fix zone states in the future...
			assert(0);
			for (i = 0; i < mgmt_io_ctx->zone_mgmt.num_zones; i++) {
				detzone_ns->internal.zone[zone_idx + i].state = SPDK_BDEV_ZONE_STATE_OFFLINE;
				vbdev_detzone_ns_dealloc_zone(detzone_ns, zone_idx + i);
			}
		}
		spdk_thread_send_msg(mgmt_io_ctx->submited_thread,
									 _detzone_zone_management_complete, mgmt_io_ctx);
	}

	spdk_bdev_free_io(bdev_io);
	return;
}

static void
_vbdev_detzone_reserve_zone_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)cb_arg;
	struct vbdev_detzone_zone_info *zone;

	assert(success);
	zone = TAILQ_FIRST(&detzone_ctrlr->zone_empty);
	assert(zone->zone_id == bdev_io->u.bdev.offset_blocks
			&& bdev_io->u.bdev.num_blocks == 1);

	detzone_ctrlr->num_zone_empty--;
	TAILQ_REMOVE(&detzone_ctrlr->zone_empty, zone, link);

	assert(zone->write_pointer == zone->zone_id);
	zone->write_pointer += DETZONE_RESERVATION_BLKS;
	zone->state = SPDK_BDEV_ZONE_STATE_IMP_OPEN;

	TAILQ_INSERT_TAIL(&detzone_ctrlr->zone_reserved, zone, link);
	detzone_ctrlr->num_zone_reserved++;
	detzone_ctrlr->zone_alloc_cnt++;
	spdk_bdev_free_io(bdev_io);
	//SPDK_DEBUGLOG(vbdev_detzone, "reserved a zone: zone_id(0x%lx @ %p) num_rsvd(%u) num_empty(%u)\n",
	//					 zone->zone_id, zone, detzone_ctrlr->num_zone_reserved, detzone_ctrlr->num_zone_empty);

	// try one more (will be ignored if we already have enough)
	vbdev_detzone_reserve_zone(detzone_ctrlr);
	return;
}

/**
 * @brief 
 * detzone_ctrlr has responsibility for ZONE allocation commands.
 * each namespace should send_msg to detzone_ctrlr thread for the zone management.
 * once it completes the request, wake up the calling thread to resume I/O.
 */
static void
vbdev_detzone_reserve_zone(void *arg)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)arg;
	struct vbdev_detzone_zone_info *zone;
	int rc;

	assert(detzone_ctrlr->thread == spdk_get_thread());
	if (detzone_ctrlr->num_zone_reserved >= detzone_ctrlr->num_pu) {
		return;
	}
	zone = TAILQ_FIRST(&detzone_ctrlr->zone_empty);
	if (!zone) {
		// TODO: it's overprovisioning case. we don't support yet.
		assert(0);
	}
	if (zone->pu_group != detzone_ctrlr->num_pu) {
		SPDK_DEBUGLOG(vbdev_detzone, "ongoing reservation process found (zone_id:0x%lx)\n",
												zone->zone_id);
		// there is an on-going reservation. ignore redundant request.
		return;
	}
	//SPDK_DEBUGLOG(vbdev_detzone, "reserving a zone (zone_id:0x%lx @ %p)\n",
	//										zone->zone_id, zone);
	zone->pu_group = (detzone_ctrlr->zone_alloc_cnt + 1) % detzone_ctrlr->num_pu;
	rc = spdk_bdev_write_zeroes_blocks(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
										zone->zone_id, DETZONE_RESERVATION_BLKS, _vbdev_detzone_reserve_zone_cb,
										detzone_ctrlr);
	assert(!rc);
	return;
}

static void
_vbdev_detzone_md_read_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone_md_io_ctx *md_io_ctx = cb_arg;
	struct vbdev_detzone *detzone_ctrlr = md_io_ctx->detzone_ctrlr;
	struct vbdev_detzone_zone_md *zone_md;
	uint64_t zone_idx;

	spdk_bdev_free_io(bdev_io);

	if (!success) {
		md_io_ctx->cb(md_io_ctx->cb_arg, false);
		goto complete;
	}

	zone_idx = md_io_ctx->zslba / detzone_ctrlr->mgmt_bdev.zone_size;
	zone_md = (struct vbdev_detzone_zone_md *)md_io_ctx->buf;
	md_io_ctx->zone_md[zone_idx].ns_id = zone_md->ns_id;
	md_io_ctx->zone_md[zone_idx].lzone_id = zone_md->lzone_id;
	md_io_ctx->zone_md[zone_idx].stripe_id = zone_md->stripe_id;
	md_io_ctx->zone_md[zone_idx].stripe_width = zone_md->stripe_width;
	md_io_ctx->zone_md[zone_idx].stripe_size = zone_md->stripe_size;

	md_io_ctx->remaining_zones--;
	if (md_io_ctx->remaining_zones == 0) {
		md_io_ctx->cb(md_io_ctx->cb_arg, true);
		goto complete;
	}
	md_io_ctx->zslba += detzone_ctrlr->mgmt_bdev.zone_size;
	_vbdev_detzone_md_read_submit(md_io_ctx);
	return;

complete:
	spdk_dma_free(md_io_ctx->buf);
	free(md_io_ctx);
}

static void
_vbdev_detzone_md_read_submit(struct vbdev_detzone_md_io_ctx *md_io_ctx)
{
	struct vbdev_detzone *detzone_ctrlr = md_io_ctx->detzone_ctrlr;
	int rc;

	md_io_ctx->buf = spdk_dma_zmalloc(detzone_ctrlr->mgmt_bdev.blocklen *
										 DETZONE_INLINE_META_BLKS, 0, NULL);
	rc = spdk_bdev_read_blocks(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
					md_io_ctx->buf,
					md_io_ctx->zslba + DETZONE_RESERVATION_BLKS,
					DETZONE_INLINE_META_BLKS,
					_vbdev_detzone_md_read_cb, md_io_ctx);
	if (rc) {
		md_io_ctx->cb(md_io_ctx->cb_arg, false);
		spdk_dma_free(md_io_ctx->buf);
		free(md_io_ctx);
	}
}

static int
vbdev_detzone_md_read(struct vbdev_detzone *detzone_ctrlr, struct vbdev_detzone_zone_md *zone_md,
								uint64_t zslba, uint64_t num_zones,
								detzone_md_io_completion_cb cb, void *cb_arg)
{
	struct vbdev_detzone_md_io_ctx *md_io_ctx;

	assert(cb);
	assert(detzone_ctrlr->thread == spdk_get_thread());

	if (zslba % detzone_ctrlr->mgmt_bdev.zone_size) {
		return -EINVAL;
	}
	
	md_io_ctx = calloc(1, sizeof(struct vbdev_detzone_md_io_ctx));
	if (!md_io_ctx) {
		return -ENOMEM;
	}
	md_io_ctx->detzone_ctrlr = detzone_ctrlr;
	md_io_ctx->zone_md = zone_md;
	md_io_ctx->zslba = zslba;
	md_io_ctx->remaining_zones = num_zones;
	md_io_ctx->cb = cb;
	md_io_ctx->cb_arg = cb_arg;

	_vbdev_detzone_md_read_submit(md_io_ctx);
	return 0;
}

static void
_vbdev_detzone_ns_alloc_md_write_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = cb_arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	uint64_t zone_idx;

	if (!success) {
		// TODO: how to handle partial errors? set to READ_ONLY?
		mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
		spdk_bdev_io_get_nvme_status(bdev_io, &mgmt_io_ctx->nvme_status.cdw0,
							 &mgmt_io_ctx->nvme_status.sct, &mgmt_io_ctx->nvme_status.sc);
	}

	_vbdev_detzone_ns_forward_phy_zone_wp(detzone_ns, bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);
	assert(mgmt_io_ctx->outstanding_mgmt_ios);
	mgmt_io_ctx->outstanding_mgmt_ios--;

	if (mgmt_io_ctx->outstanding_mgmt_ios == 0 && mgmt_io_ctx->remain_ios == 0) {
		if (mgmt_io_ctx->status != SPDK_BDEV_IO_STATUS_FAILED) {
			zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
			SPDK_DEBUGLOG(vbdev_detzone, "ALLOC: acquire open(%u) active(%u)\n", 
									detzone_ns->num_open_zones, detzone_ns->num_active_zones);
			detzone_ns->num_open_zones++;
			detzone_ns->num_active_zones++;	
			if (mgmt_io_ctx->parent_io->type == SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT) {
				detzone_ns->internal.zone[zone_idx].state = SPDK_BDEV_ZONE_STATE_EXP_OPEN;
			} else {
				detzone_ns->internal.zone[zone_idx].state = SPDK_BDEV_ZONE_STATE_IMP_OPEN;
			}
			detzone_ns->internal.zone[zone_idx].tb_last_update_tsc = spdk_get_ticks();
			detzone_ns->internal.zone[zone_idx].tb_tokens = detzone_ns->zone_stripe_tb_size * detzone_ns->zone_stripe_width;
		}
		spdk_thread_send_msg(mgmt_io_ctx->submited_thread,
									 _detzone_zone_management_complete, mgmt_io_ctx);
	}
	spdk_bdev_free_io(bdev_io);
}

static void
_vbdev_detzone_ns_alloc_md_write(struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx)
{
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	struct vbdev_detzone_zone_info *zone;
	struct vbdev_detzone_zone_md *zone_md;
	uint64_t zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	uint64_t base_zone_idx;
	uint32_t i;
	int rc;

	assert(!mgmt_io_ctx->zone_mgmt.select_all);
	mgmt_io_ctx->zone_mgmt.buf = spdk_dma_zmalloc((detzone_ns->padding_blocks - DETZONE_RESERVATION_BLKS) * 
													detzone_ns->detzone_ns_bdev.blocklen * 
													detzone_ns->zone_stripe_width,
													detzone_ns->detzone_ns_bdev.blocklen, NULL);
	mgmt_io_ctx->remain_ios = detzone_ns->zone_stripe_width;
	for (i = 0; i < detzone_ns->zone_stripe_width; i++) {
		base_zone_idx = detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id /
															 detzone_ctrlr->mgmt_bdev.zone_size;
		zone = &detzone_ctrlr->zone_info[base_zone_idx];
		zone_md = (struct vbdev_detzone_zone_md *)((uint8_t*)mgmt_io_ctx->zone_mgmt.buf +
													 i * DETZONE_INLINE_META_BLKS *
													 detzone_ns->detzone_ns_bdev.blocklen);
		zone_md->lzone_id = zone->lzone_id;
		zone_md->ns_id = zone->ns_id;
		zone_md->stripe_size = zone->stripe_size;
		zone_md->stripe_width = zone->stripe_width;
		zone_md->stripe_id = zone->stripe_id;
		// write metadata
		rc = spdk_bdev_write_blocks(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
						zone_md,
						zone->zone_id + DETZONE_RESERVATION_BLKS,
						detzone_ns->padding_blocks - DETZONE_RESERVATION_BLKS,
						_vbdev_detzone_ns_alloc_md_write_cb, mgmt_io_ctx);
		mgmt_io_ctx->outstanding_mgmt_ios++;
		mgmt_io_ctx->remain_ios--;
		assert(!rc);
	}
}

static void
vbdev_detzone_ns_alloc_zone(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = (struct vbdev_detzone_ns_mgmt_io_ctx *)arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	struct vbdev_detzone_zone_info *zone, *tmp_zone;
	//bool do_notification = false;
	uint64_t zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	uint32_t num_zone_alloc = 0;

	assert(detzone_ctrlr->thread == spdk_get_thread());
	assert(mgmt_io_ctx->zone_mgmt.num_zones == 1);

	// this should not fail...
	mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_GENERIC;
	mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_SUCCESS;

	if (spdk_unlikely(detzone_ctrlr->num_zone_reserved < detzone_ns->zone_stripe_width)) {
		// reschdule
		spdk_thread_send_msg(detzone_ctrlr->thread, vbdev_detzone_ns_alloc_zone, arg);
		return;
	}
	
	TAILQ_FOREACH_SAFE(zone, &detzone_ctrlr->zone_reserved, link, tmp_zone) {
		if (spdk_bit_array_get(detzone_ns->internal.epoch_pu_map, zone->pu_group)) {
			continue;
		}
		detzone_ns->internal.zone[zone_idx].base_zone[num_zone_alloc].zone_id = zone->zone_id;
		zone->ns_id = detzone_ns->nsid;
		zone->lzone_id = detzone_ns->internal.zone[zone_idx].zone_id;
		zone->stripe_id = num_zone_alloc;
		zone->stripe_size = detzone_ns->zone_stripe_blks;
		zone->stripe_width = detzone_ns->zone_stripe_width;
		spdk_bit_array_set(detzone_ns->internal.epoch_pu_map, zone->pu_group);

		num_zone_alloc++;
		detzone_ns->internal.epoch_num_pu++;
		TAILQ_REMOVE(&detzone_ctrlr->zone_reserved, zone, link);

		if (detzone_ns->internal.epoch_num_pu >= detzone_ctrlr->num_pu) {
			spdk_bit_array_clear_mask(detzone_ns->internal.epoch_pu_map);
			detzone_ns->internal.epoch_num_pu = 0;
		}
		if (num_zone_alloc == detzone_ns->zone_stripe_width) {
			break;
		}
	}
	detzone_ctrlr->num_zone_reserved -= num_zone_alloc;

	assert((detzone_ns->zone_stripe_width - num_zone_alloc) <= detzone_ctrlr->num_zone_reserved);
	while (num_zone_alloc < detzone_ns->zone_stripe_width) {
		zone = TAILQ_FIRST(&detzone_ctrlr->zone_reserved);
		detzone_ns->internal.zone[zone_idx].base_zone[num_zone_alloc].zone_id = zone->zone_id;
		zone->ns_id = detzone_ns->nsid;
		zone->lzone_id = detzone_ns->internal.zone[zone_idx].zone_id;
		zone->stripe_id = num_zone_alloc;
		zone->stripe_size = detzone_ns->zone_stripe_blks;
		zone->stripe_width = detzone_ns->zone_stripe_width;
		spdk_bit_array_set(detzone_ns->internal.epoch_pu_map, zone->pu_group);

		num_zone_alloc++;
		detzone_ns->internal.epoch_num_pu++;
		TAILQ_REMOVE(&detzone_ctrlr->zone_reserved, zone, link);
		if (detzone_ns->internal.epoch_num_pu >= detzone_ctrlr->num_pu) {
			spdk_bit_array_clear_mask(detzone_ns->internal.epoch_pu_map);
			detzone_ns->internal.epoch_num_pu = 0;
		}
	}
	
	_vbdev_detzone_ns_alloc_md_write(mgmt_io_ctx);
	// We reschedule new reservations to give a chance to this allocation completes first.
	spdk_thread_send_msg(detzone_ctrlr->thread, vbdev_detzone_reserve_zone, detzone_ctrlr);
}

static void
_vbdev_detzone_update_phy_zone_info_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone_update_ctx *ctx = cb_arg;
	struct vbdev_detzone *detzone_ctrlr = ctx->detzone_ctrlr;
	uint64_t zone_idx = bdev_io->u.zone_mgmt.zone_id / detzone_ctrlr->mgmt_bdev.zone_size;

	if (success) {
		// TODO: check if the current physical zone info matches to the logical zone info
		detzone_ctrlr->zone_info[zone_idx].state = ctx->info.state;
		detzone_ctrlr->zone_info[zone_idx].write_pointer = ctx->info.write_pointer;
		if (detzone_ctrlr->zone_info[zone_idx].state == SPDK_BDEV_ZONE_STATE_EMPTY) {
			assert(detzone_ctrlr->zone_info[zone_idx].ns_id == 0);
			detzone_ctrlr->zone_info[zone_idx].pu_group = detzone_ctrlr->num_pu;
			TAILQ_INSERT_TAIL(&detzone_ctrlr->zone_empty,
								 &detzone_ctrlr->zone_info[zone_idx], link);
			detzone_ctrlr->num_zone_empty++;
		}
	}

	free(ctx);
	spdk_bdev_free_io(bdev_io);
}

static void
_vbdev_detzone_update_phy_zone_info(struct vbdev_detzone *detzone_ctrlr,
							struct vbdev_detzone_ns *detzone_ns, uint64_t zone_id)
{
	struct vbdev_detzone_update_ctx *ctx;
	int rc;

	ctx = calloc(1, sizeof(struct vbdev_detzone_update_ctx));
	ctx->detzone_ctrlr = detzone_ctrlr;
	ctx->detzone_ns = detzone_ns;
	rc = spdk_bdev_get_zone_info(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
					zone_id, 1, &ctx->info,
					_vbdev_detzone_update_phy_zone_info_cb, ctx);
	assert(!rc);
}

static void
vbdev_detzone_ns_dealloc_zone(struct vbdev_detzone_ns *detzone_ns, uint64_t zone_idx)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	uint64_t dealloc_zone_idx;
	uint32_t i;

	assert(detzone_ctrlr->thread == spdk_get_thread());

	for (i = 0; i < detzone_ns->zone_stripe_width; i++) {
		assert(detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id != UINT64_MAX);
		dealloc_zone_idx = detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id /
								detzone_ctrlr->mgmt_bdev.zone_size;
		detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id = UINT64_MAX;
		detzone_ctrlr->zone_info[dealloc_zone_idx].ns_id = 0;
		detzone_ctrlr->zone_info[dealloc_zone_idx].lzone_id = 0;
		detzone_ctrlr->zone_info[dealloc_zone_idx].stripe_id = 0;
		detzone_ctrlr->zone_info[dealloc_zone_idx].stripe_width = 0;
		detzone_ctrlr->zone_info[dealloc_zone_idx].stripe_size = 0;
		detzone_ctrlr->zone_info[dealloc_zone_idx].pu_group = detzone_ctrlr->num_pu;

		// !!!! NEED CHECK AGAIN !!!!
		if (detzone_ns->internal.zone[zone_idx].state == SPDK_BDEV_ZONE_STATE_EMPTY) {
			assert(detzone_ctrlr->zone_info[dealloc_zone_idx].state == SPDK_BDEV_ZONE_STATE_EMPTY);
			detzone_ctrlr->zone_info[dealloc_zone_idx].write_pointer =
									 detzone_ctrlr->zone_info[dealloc_zone_idx].zone_id;
			TAILQ_INSERT_TAIL(&detzone_ctrlr->zone_empty,
									&detzone_ctrlr->zone_info[dealloc_zone_idx], link);
			detzone_ctrlr->num_zone_empty++;
		} else {
			assert(detzone_ns->internal.zone[zone_idx].state == SPDK_BDEV_ZONE_STATE_OFFLINE);
			// We have to check the state of the physical zone for cases other than EMPTY (by RESET)
			_vbdev_detzone_update_phy_zone_info(detzone_ctrlr, detzone_ns, detzone_ctrlr->zone_info[dealloc_zone_idx].zone_id);
		}
	}
}

/*
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
		if (detzone_ctrlr->num_open_states + detzone_ns->zone_stripe_width
						<= detzone_ctrlr->base_bdev->max_open_zones) {
			detzone_ctrlr->num_open_states += detzone_ns->zone_stripe_width;
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
			detzone_ctrlr->num_open_states -= detzone_ns->zone_stripe_width;
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
*/

static void
vbdev_detzone_ns_imp_open_zone(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	uint64_t zone_idx, detzone_idx;
	uint32_t i;

	assert(detzone_ctrlr->thread == spdk_get_thread());
	assert(mgmt_io_ctx->parent_io->type != SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT);

	zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	if (detzone_ns->internal.zone[zone_idx].state == SPDK_BDEV_ZONE_STATE_CLOSED) {
		detzone_ns->internal.zone[zone_idx].state = SPDK_BDEV_ZONE_STATE_IMP_OPEN;
		SPDK_DEBUGLOG(vbdev_detzone, "IMP_OPEN: acquire open(%u)\n", 
								detzone_ns->num_open_zones);
		for (i = 0; i < detzone_ns->zone_stripe_width; i++) {
			detzone_idx = detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id /
								detzone_ctrlr->mgmt_bdev.zone_size;
			if (detzone_ctrlr->zone_info[detzone_idx].state == SPDK_BDEV_ZONE_STATE_CLOSED) {
				detzone_ctrlr->zone_info[detzone_idx].state = SPDK_BDEV_ZONE_STATE_IMP_OPEN;
			}
		}
		detzone_ns->internal.zone[zone_idx].tb_last_update_tsc = spdk_get_ticks();
		detzone_ns->internal.zone[zone_idx].tb_tokens = detzone_ns->zone_stripe_tb_size * detzone_ns->zone_stripe_width;
		detzone_ns->num_open_zones++;
		goto complete;
	}
	assert(detzone_ns->internal.zone[zone_idx].state == SPDK_BDEV_ZONE_STATE_EMPTY);
	if (detzone_ns->num_open_zones >= detzone_ns->detzone_ns_bdev.max_open_zones) {
		SPDK_ERRLOG("Too many open zones (%u)\n", detzone_ns->num_open_zones);
		mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
		mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_TOO_MANY_OPEN;
	} else if (detzone_ns->num_active_zones >= detzone_ns->detzone_ns_bdev.max_active_zones) {
		SPDK_ERRLOG("Too many active zones (%u)\n", detzone_ns->num_active_zones);
		mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
		mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_TOO_MANY_ACTIVE;
	} else {
		vbdev_detzone_ns_alloc_zone(mgmt_io_ctx);
		return;
	}

complete:
	spdk_thread_send_msg(mgmt_io_ctx->submited_thread,
										 _detzone_zone_management_complete, mgmt_io_ctx);
}

static void
vbdev_detzone_ns_open_zone(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	uint64_t zone_idx, detzone_idx;
	uint32_t i;
	int rc;

	zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	if (!mgmt_io_ctx->zone_mgmt.select_all) {
		switch (detzone_ns->internal.zone[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
			goto complete;
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
			detzone_ns->internal.zone[zone_idx].state = SPDK_BDEV_ZONE_STATE_EXP_OPEN;
			goto complete;
		case SPDK_BDEV_ZONE_STATE_EMPTY:
			if (detzone_ns->num_open_zones >= detzone_ns->detzone_ns_bdev.max_open_zones) {
				mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
				mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_TOO_MANY_OPEN;
				goto complete;
			} else if (detzone_ns->num_active_zones >= detzone_ns->detzone_ns_bdev.max_active_zones) {
				mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
				mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_TOO_MANY_ACTIVE;
				goto complete;
			} else {
				vbdev_detzone_ns_alloc_zone(mgmt_io_ctx);
			}
			return;
		case SPDK_BDEV_ZONE_STATE_CLOSED:
			break;
		default:
			mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_INVALID_STATE;
			goto complete;
		}
	}

	while (mgmt_io_ctx->remain_ios > 0) {
		switch (detzone_ns->internal.zone[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_CLOSED:
			for (i = 0, rc = 0; i < detzone_ns->zone_stripe_width && rc == 0; i++) {
				detzone_idx = detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id /
								detzone_ctrlr->mgmt_bdev.zone_size;
				switch (detzone_ctrlr->zone_info[detzone_idx].state) {
				case SPDK_BDEV_ZONE_STATE_CLOSED:
					if (!(rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
									detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id,
									mgmt_io_ctx->zone_mgmt.zone_action, _detzone_zone_management_done, mgmt_io_ctx))) {
						mgmt_io_ctx->outstanding_mgmt_ios++;
						mgmt_io_ctx->remain_ios--;
					}
					break;
				default:
					mgmt_io_ctx->remain_ios--;
					break;
				}
			}
			if (rc != 0) {
				mgmt_io_ctx->remain_ios = 0;
				if (mgmt_io_ctx->outstanding_mgmt_ios != 0) {
					// error will be handled by completing IOs
					return;
				}
				goto complete;
			}
			detzone_ns->internal.zone[zone_idx].tb_last_update_tsc = spdk_get_ticks();
			detzone_ns->internal.zone[zone_idx].tb_tokens = detzone_ns->zone_stripe_tb_size * detzone_ns->zone_stripe_width;
			break;
		default:
			// skip zones in other states
			mgmt_io_ctx->remain_ios -= detzone_ns->zone_stripe_width;
			break;
		}
		zone_idx++;
	}
	return;

complete:
	spdk_thread_send_msg(mgmt_io_ctx->submited_thread,
										 _detzone_zone_management_complete, mgmt_io_ctx);
	return;
}

static void
vbdev_detzone_ns_reset_zone(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	uint64_t zone_idx, detzone_idx;
	uint32_t i;
	int rc;

	zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	SPDK_DEBUGLOG(vbdev_detzone, "reset zone_id(0x%lx) curr_state(%u)\n",
									 		mgmt_io_ctx->zone_mgmt.zone_id,
											detzone_ns->internal.zone[zone_idx].state);
	if (!mgmt_io_ctx->zone_mgmt.select_all) {
		switch (detzone_ns->internal.zone[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
		case SPDK_BDEV_ZONE_STATE_CLOSED:
		case SPDK_BDEV_ZONE_STATE_FULL:
			break;
		case SPDK_BDEV_ZONE_STATE_EMPTY:
			goto complete;
		default:
			mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_INVALID_STATE;
			goto complete;
		}
	}

	while (mgmt_io_ctx->remain_ios > 0) {
		switch (detzone_ns->internal.zone[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
		case SPDK_BDEV_ZONE_STATE_CLOSED:
		case SPDK_BDEV_ZONE_STATE_FULL:
			for (i = 0, rc = 0; i < detzone_ns->zone_stripe_width && rc == 0; i++) {
				detzone_idx = detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id /
								detzone_ctrlr->mgmt_bdev.zone_size;
				switch (detzone_ctrlr->zone_info[detzone_idx].state) {
				case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
				case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
				case SPDK_BDEV_ZONE_STATE_CLOSED:
				case SPDK_BDEV_ZONE_STATE_FULL:
					if (!(rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
									detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id,
									mgmt_io_ctx->zone_mgmt.zone_action, _detzone_zone_management_done, mgmt_io_ctx))) {
						mgmt_io_ctx->outstanding_mgmt_ios++;
						mgmt_io_ctx->remain_ios--;
					}
					break;
				default:
					mgmt_io_ctx->remain_ios--;
					break;
				}
			}
			if (rc != 0) {
				mgmt_io_ctx->remain_ios = 0;
				if (mgmt_io_ctx->outstanding_mgmt_ios != 0) {
					// error will be handled by completing IOs
					return;
				}
				goto complete;
			}
			break;
		default:
			// skip zones in other states
			mgmt_io_ctx->remain_ios -= detzone_ns->zone_stripe_width;
			break;
		}
		zone_idx++;
	}
	return;

complete:
	spdk_thread_send_msg(mgmt_io_ctx->submited_thread,
										 _detzone_zone_management_complete, mgmt_io_ctx);
	return;
}

static void
vbdev_detzone_ns_close_zone(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	uint64_t zone_idx, detzone_idx;
	uint32_t i;
	int rc;

	zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	if (!mgmt_io_ctx->zone_mgmt.select_all) {
		switch (detzone_ns->internal.zone[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
			break;
		case SPDK_BDEV_ZONE_STATE_CLOSED:
			goto complete;
		default:
			mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_INVALID_STATE;
			goto complete;
		}
	}

	while (mgmt_io_ctx->remain_ios > 0) {
		switch (detzone_ns->internal.zone[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
			for (i = 0, rc = 0; i < detzone_ns->zone_stripe_width && rc == 0; i++) {
				detzone_idx = detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id /
								detzone_ctrlr->mgmt_bdev.zone_size;
				switch (detzone_ctrlr->zone_info[detzone_idx].state) {
				case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
				case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
					if (!(rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
									detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id,
									mgmt_io_ctx->zone_mgmt.zone_action, _detzone_zone_management_done, mgmt_io_ctx))) {
						mgmt_io_ctx->outstanding_mgmt_ios++;
						mgmt_io_ctx->remain_ios--;
					}
					break;
				default:
					mgmt_io_ctx->remain_ios--;
					break;
				}
			}
			if (rc != 0) {
				mgmt_io_ctx->remain_ios = 0;
				if (mgmt_io_ctx->outstanding_mgmt_ios != 0) {
					// error will be handled by completing IOs
					return;
				}
				goto complete;
			}
			break;
		default:
			// skip zones in other states
			mgmt_io_ctx->remain_ios -= detzone_ns->zone_stripe_width;
			break;
		}
		zone_idx++;
	}
	return;

complete:
	spdk_thread_send_msg(mgmt_io_ctx->submited_thread,
										 _detzone_zone_management_complete, mgmt_io_ctx);
	return;
}

static void
vbdev_detzone_ns_finish_zone(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	uint64_t zone_idx, detzone_idx;
	uint32_t i;
	int rc;

	zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	if (!mgmt_io_ctx->zone_mgmt.select_all) {
		switch (detzone_ns->internal.zone[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
		case SPDK_BDEV_ZONE_STATE_CLOSED:
			break;
		case SPDK_BDEV_ZONE_STATE_FULL:
			goto complete;
		default:
			mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_INVALID_STATE;
			goto complete;
		}
	}

	while (mgmt_io_ctx->remain_ios > 0) {
		switch (detzone_ns->internal.zone[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
		case SPDK_BDEV_ZONE_STATE_CLOSED:
			for (i = 0, rc = 0; i < detzone_ns->zone_stripe_width && rc == 0; i++) {
				detzone_idx = detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id /
								detzone_ctrlr->mgmt_bdev.zone_size;
				switch (detzone_ctrlr->zone_info[detzone_idx].state) {
				case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
				case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
				case SPDK_BDEV_ZONE_STATE_CLOSED:
					if (!(rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
									detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id,
									mgmt_io_ctx->zone_mgmt.zone_action, _detzone_zone_management_done, mgmt_io_ctx))) {
						mgmt_io_ctx->outstanding_mgmt_ios++;
						mgmt_io_ctx->remain_ios--;
					}
					break;
				default:
					mgmt_io_ctx->remain_ios--;
					break;
				}
			}
			if (rc != 0) {
				mgmt_io_ctx->remain_ios = 0;
				if (mgmt_io_ctx->outstanding_mgmt_ios != 0) {
					// error will be handled by completing IOs
					return;
				}
				goto complete;
			}
			break;
		default:
			// skip zones in other states
			mgmt_io_ctx->remain_ios -= detzone_ns->zone_stripe_width;
			break;
		}
		zone_idx++;
	}
	return;

complete:
	spdk_thread_send_msg(mgmt_io_ctx->submited_thread,
										 _detzone_zone_management_complete, mgmt_io_ctx);
	return;
}

static void
vbdev_detzone_ns_offline_zone(void *arg)
{
	assert(0);
	/* TODO: we don't consider READ_ONLY or OFFLINE zones for now
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	uint64_t zone_idx;

	zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	if (!mgmt_io_ctx->zone_mgmt.select_all) {
		switch (detzone_ns->internal.zone[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_OFFLINE:
			goto complete;
		case SPDK_BDEV_ZONE_STATE_READ_ONLY:
			break;
		default:
			mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_INVALID_STATE;
			goto complete;
		}
	}

	while (mgmt_io_ctx->remain_ios > 0) {
		switch (detzone_ns->internal.zone[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_READ_ONLY:
			detzone_ns->internal.zone[zone_idx].state = SPDK_NVME_ZONE_STATE_OFFLINE;
			vbdev_detzone_ns_dealloc_zone(detzone_ns, zone_idx);
			break;
		default:
			// skip zones in other states
			break;
		}
		zone_idx++;
		mgmt_io_ctx->remain_ios--;
	}
	return;

complete:
	spdk_thread_send_msg(spdk_bdev_io_get_thread(mgmt_io_ctx->parent_io),
										 _detzone_zone_management_complete, mgmt_io_ctx);
	return;
	*/
}

static int
_detzone_ns_io_write_split(struct vbdev_detzone_ns *detzone_ns, struct spdk_bdev_io *bdev_io)
{
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	uint64_t blks_to_split;
	int rc = 0;

	while (io_ctx->u.io.remain_blocks) {
		// TODO: use '&' operator rather than '%'
		blks_to_split = spdk_min(io_ctx->u.io.remain_blocks,
						detzone_ns->zone_stripe_blks - (io_ctx->u.io.next_offset_blocks % detzone_ns->zone_stripe_blks));	

		if (spdk_unlikely(blks_to_split * detzone_ns->detzone_ns_bdev.blocklen >
									bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len - io_ctx->u.io.iov_offset)) {
			blks_to_split = (bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len - io_ctx->u.io.iov_offset) /
																	detzone_ns->detzone_ns_bdev.blocklen;
		}
		assert(blks_to_split>0);

		rc = _vbdev_detzone_ns_append_zone_iov(detzone_ns, io_ctx->u.io.next_offset_blocks,
									 bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_base + io_ctx->u.io.iov_offset,
									 blks_to_split);

		if (rc == 0) {
			io_ctx->u.io.next_offset_blocks += blks_to_split;
			io_ctx->u.io.remain_blocks -= blks_to_split;
			io_ctx->u.io.iov_offset += blks_to_split * detzone_ns->detzone_ns_bdev.blocklen;
			assert(io_ctx->u.io.iov_offset <= bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len);
			if (io_ctx->u.io.iov_offset == bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len) {
				io_ctx->u.io.iov_offset = 0;
				io_ctx->u.io.iov_idx++;
			}
		} else {
			break;
		}
	}

	return rc;
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

static void
_detzone_ns_io_complete(void *arg)
{
	struct detzone_bdev_io *io_ctx = arg;
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(io_ctx);

	if (io_ctx->nvme_status.sct || io_ctx->nvme_status.sc) {
		spdk_bdev_io_complete_nvme_status(bdev_io,
											 io_ctx->nvme_status.cdw0,
											 io_ctx->nvme_status.sct,
											 io_ctx->nvme_status.sc);
	} else {
		spdk_bdev_io_complete(bdev_io, io_ctx->status);
	}
}

/* Completion callback for read IO that were issued from this bdev. The original bdev_io
 * is passed in as an arg so we'll complete that one with the appropriate status
 * and then free the one that this module issued.
 */
static void
_detzone_ns_io_read_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;
	//struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(orig_io->bdev, struct vbdev_detzone_ns,
	//				 detzone_ns_bdev);
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)orig_io->driver_ctx;
	//struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(io_ctx->ch);

	if (!success) {
		io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
		spdk_bdev_io_get_nvme_status(bdev_io, &io_ctx->nvme_status.cdw0, &io_ctx->nvme_status.sct, &io_ctx->nvme_status.sc);
		SPDK_ERRLOG("Partial I/O has failed: sct(0x%2x) sc(0x%2x)\n", io_ctx->nvme_status.sct, io_ctx->nvme_status.sc);
	}

	assert(io_ctx->u.io.outstanding_stripe_ios);
	io_ctx->u.io.outstanding_stripe_ios--;

	if (io_ctx->u.io.outstanding_stripe_ios == 0 && io_ctx->u.io.remain_blocks == 0) {
		if (io_ctx->status != SPDK_BDEV_IO_STATUS_FAILED) {
			io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
		}
		_detzone_ns_io_complete(io_ctx);
	}

	spdk_bdev_free_io(bdev_io);
	return;
}

static int
_detzone_ns_io_read_submit(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns,
					 detzone_ns_bdev);
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;

	int rc = 0;
	uint64_t phy_offset_blks;
	uint64_t blks_to_submit;

	while (io_ctx->u.io.remain_blocks) {
		// TODO: use '&' operator rather than '%'
		phy_offset_blks = _vbdev_detzone_ns_get_phy_offset(detzone_ns, io_ctx->u.io.next_offset_blocks);
		if (phy_offset_blks == UINT64_MAX ||
				io_ctx->u.io.next_offset_blocks % detzone_ns->detzone_ns_bdev.zone_size > detzone_ns->zcap) {
			blks_to_submit = spdk_min(detzone_ns->detzone_ns_bdev.zone_size - 
								 (io_ctx->u.io.next_offset_blocks % detzone_ns->detzone_ns_bdev.zone_size),
							 (bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len - io_ctx->u.io.iov_offset) /
							 									 detzone_ns->detzone_ns_bdev.blocklen);
			if (spdk_unlikely(blks_to_submit > io_ctx->u.io.remain_blocks)) {
				blks_to_submit = io_ctx->u.io.remain_blocks;
			}
			// TODO: behavior should match with the device (i.e., DLFEAT bit)
			// Currently, we return the buffer as is (possibly undefined)

			// We complete I/O immediately if no more blocks to read and previous I/Os have completed.
			// Otherwise, _detzone_ns_complete_io() will handle the completion.
			if (io_ctx->u.io.outstanding_stripe_ios == 0 && io_ctx->u.io.remain_blocks - blks_to_submit == 0) {
				io_ctx->u.io.remain_blocks = 0;
				if (io_ctx->status != SPDK_BDEV_IO_STATUS_FAILED) {
					io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
				}
				_detzone_ns_io_complete(io_ctx);
				goto out;
			}
		} else {
			blks_to_submit = spdk_min(io_ctx->u.io.remain_blocks,
							detzone_ns->zone_stripe_blks - (io_ctx->u.io.next_offset_blocks % detzone_ns->zone_stripe_blks));
			
			// We reuse allocated iovs instead of trying to get new one. 
			// It is likely aligned with the stripes
			if (spdk_unlikely(blks_to_submit * detzone_ns->detzone_ns_bdev.blocklen >
									 bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len - io_ctx->u.io.iov_offset)) {
				blks_to_submit = (bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len - io_ctx->u.io.iov_offset) /
																	 detzone_ns->detzone_ns_bdev.blocklen;
			}

			// TODO: check if this namespace blocklen is not equal to the base blocklen.
			// detzone_ns_bdev.phys_blocklen != detzone_ns_bdev.blocklen
			// If so, convert it here
			assert(blks_to_submit>0);

			rc = spdk_bdev_read_blocks(detzone_ctrlr->base_desc, detzone_ch->base_ch,
							bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_base + io_ctx->u.io.iov_offset,
							phy_offset_blks, blks_to_submit, _detzone_ns_io_read_complete,
							bdev_io);
			if (rc == 0) {
				io_ctx->u.io.outstanding_stripe_ios++;
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
			io_ctx->u.io.next_offset_blocks += blks_to_submit;
			io_ctx->u.io.remain_blocks -= blks_to_submit;

			io_ctx->u.io.iov_offset += blks_to_submit * detzone_ns->detzone_ns_bdev.blocklen;
			assert(io_ctx->u.io.iov_offset <= bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len);
			if (io_ctx->u.io.iov_offset == bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len) {
				io_ctx->u.io.iov_offset = 0;
				io_ctx->u.io.iov_idx++;
			}
		}
	}

out:
	if (rc == 0 ) {
		return rc;
	}

error_out:
	io_ctx->u.io.remain_blocks = 0;
	io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
	if (io_ctx->u.io.outstanding_stripe_ios != 0) {
		// defer error handling until all stripe ios complete
		rc = 0;
	}
	return rc;
}

static void
_detzone_ns_read_get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io, bool success)
{
	if (spdk_unlikely(!success)) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
	
	if (_detzone_ns_io_read_submit(ch, bdev_io) != 0) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
	return;
}

/* Completion callback for write IO that were issued from this bdev.
 * We'll check the stripe IO and complete original bdev_ios with the appropriate status
 * and then free the one that this module issued.
 */
static void
_detzone_ns_io_write_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone_ns_zone *zone = cb_arg;
	struct detzone_bdev_io *tmp_ctx, *io_ctx;
	struct spdk_bdev_io *orig_io;
	struct vbdev_detzone_ns *detzone_ns;
	struct detzone_io_channel *detzone_ch;
	uint64_t submit_tsc;

	// Get the namespace info using io_ctx at the head
	if (TAILQ_EMPTY(&zone->wr_waiting_cpl)) {
		io_ctx = TAILQ_FIRST(&zone->wr_pending_queue);
	} else {
		io_ctx = TAILQ_FIRST(&zone->wr_waiting_cpl);
	}
	assert(io_ctx);
	orig_io = spdk_bdev_io_from_ctx(io_ctx);
	detzone_ch = spdk_io_channel_get_ctx(io_ctx->ch);
	detzone_ns = SPDK_CONTAINEROF(orig_io->bdev, struct vbdev_detzone_ns,
					 											detzone_ns_bdev);

	io_ctx = NULL;
	orig_io = NULL;

	if (!success) {
		// TODO: we will need a recovery mechanism for this case.
		TAILQ_FOREACH(io_ctx, &zone->wr_waiting_cpl, link) {
			io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
			spdk_bdev_io_get_nvme_status(bdev_io, &io_ctx->nvme_status.cdw0, &io_ctx->nvme_status.sct, &io_ctx->nvme_status.sc);
		}
	} else {
		spdk_bdev_io_get_submit_tsc(bdev_io, &submit_tsc);
		detzone_ch->write_blks += bdev_io->u.bdev.num_blocks;
		detzone_ch->total_write_blk_tsc += spdk_get_ticks() - submit_tsc;

		_vbdev_detzone_ns_forward_phy_zone_wp(detzone_ns, bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);
	}

	zone->wr_outstanding_ios -= 1;
	if (zone->wr_outstanding_ios == 0 && zone->wr_zone_in_progress == detzone_ns->zone_stripe_width) {
		// complete original bdev_ios
		TAILQ_FOREACH_SAFE(io_ctx, &zone->wr_waiting_cpl, link, tmp_ctx) {
			TAILQ_REMOVE(&zone->wr_waiting_cpl, io_ctx, link);
			orig_io = spdk_bdev_io_from_ctx(io_ctx);
			if (io_ctx->status != SPDK_BDEV_IO_STATUS_FAILED) {
				vbdev_detzone_ns_zone_wp_forward(detzone_ns, orig_io->u.bdev.offset_blocks, orig_io->u.bdev.num_blocks);
				io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
			}
			spdk_thread_send_msg(spdk_bdev_io_get_thread(orig_io), _detzone_ns_io_complete, io_ctx);
		}

		if (TAILQ_EMPTY(&zone->wr_pending_queue)) {
			// no more write I/O to process in the batch. we remove this zone from the scheduler.
			TAILQ_REMOVE(&detzone_ns->internal.zone_write_queue, zone, link);
		}
		zone->wr_zone_in_progress = 0;
	}

	spdk_bdev_free_io(bdev_io);
	return;
}

static void
_detzone_ns_io_write_submit(struct spdk_io_channel *ch, struct vbdev_detzone_ns *detzone_ns, struct vbdev_detzone_ns_zone *zone)
{
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);
	struct detzone_bdev_io *io_ctx, *tmp_ctx;
	uint32_t i;
	int rc = 0;

	if (zone->wr_zone_in_progress == detzone_ns->zone_stripe_width) {
		return;
	}

	if (zone->wr_outstanding_ios == 0 && zone->wr_zone_in_progress == 0) {
		zone->wr_outstanding_ios = detzone_ns->zone_stripe_width;
	}

	for (i = zone->wr_zone_in_progress; i < detzone_ns->zone_stripe_width; i = ++zone->wr_zone_in_progress) {
		if (zone->base_zone[i].iov_blks == 0) {
			zone->wr_outstanding_ios -= 1;
			continue;
		} else if (rc) {
			// previous I/O has failed to submit. we fail remainings too.
			zone->wr_outstanding_ios -= 1;
			zone->base_zone[i].iov_blks = 0;
			zone->base_zone[i].iov_cnt = 0;
			continue;
		}

		if (zone->tb_tokens < zone->base_zone[i].iov_blks) {
			break;
		}

		rc = spdk_bdev_writev_blocks(detzone_ctrlr->base_desc, detzone_ch->base_ch,
						zone->base_zone[i].iovs, zone->base_zone[i].iov_cnt,
						_vbdev_detzone_ns_get_phy_zone_wp(detzone_ns, zone->base_zone[i].zone_id),
						zone->base_zone[i].iov_blks,
						_detzone_ns_io_write_complete, zone);
		if (rc == 0) {
			zone->base_zone[i].iov_blks = 0;
			zone->base_zone[i].iov_cnt = 0;
		}
	}

	if (rc) {
		// Mark all original IOs to fail
		TAILQ_FOREACH(io_ctx, &zone->wr_waiting_cpl, link) {
			io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
		}
		// If no I/O has been submitted, we have to complete here.
		if (zone->wr_outstanding_ios == 0) {
			TAILQ_FOREACH_SAFE(io_ctx, &zone->wr_waiting_cpl, link, tmp_ctx) {
				TAILQ_REMOVE(&zone->wr_waiting_cpl, io_ctx, link);
				spdk_thread_send_msg(spdk_bdev_io_get_thread(spdk_bdev_io_from_ctx(io_ctx)),
														 _detzone_ns_io_complete, io_ctx);
			}
			if (TAILQ_EMPTY(&zone->wr_pending_queue)) {
				// no more write I/O to process in the batch. we remove this zone from the scheduler.
				TAILQ_REMOVE(&detzone_ns->internal.zone_write_queue, zone, link);
			}
			zone->wr_zone_in_progress = 0;
		}
	}
}

static void
_detzone_ns_write_cb(struct spdk_bdev_io *bdev_io, int sct, int sc, void *cb_arg)
{
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns,
					 detzone_ns_bdev);
	struct spdk_io_channel *ch = cb_arg;
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	uint64_t zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, bdev_io->u.bdev.offset_blocks);
	int rc = 0;
	if (sct || sc) {
		io_ctx->nvme_status.sct = sct;
		io_ctx->nvme_status.sc = sc;
		spdk_thread_send_msg(spdk_bdev_io_get_thread(bdev_io),
													_detzone_ns_io_complete, io_ctx);
		return;
	}

	if (TAILQ_EMPTY(&detzone_ns->internal.zone[zone_idx].wr_waiting_cpl)
			&& TAILQ_EMPTY(&detzone_ns->internal.zone[zone_idx].wr_pending_queue)) {
		// This IO is the first in current batch of the zone. Add this zone to the write queue.
		TAILQ_INSERT_TAIL(&detzone_ns->internal.zone_write_queue, &detzone_ns->internal.zone[zone_idx], link);
	}

	if (TAILQ_EMPTY(&detzone_ns->internal.zone[zone_idx].wr_pending_queue)) {
		rc = _detzone_ns_io_write_split(detzone_ns, bdev_io);
		if (rc == -EAGAIN) {
			// Some portion of this IO may be submitted in the current batch, but not completely.
			// Thus, we add this IO to the pending queue and process remaining part in the next batch.
			TAILQ_INSERT_TAIL(&detzone_ns->internal.zone[zone_idx].wr_pending_queue, io_ctx, link);
		} else {
			assert(rc == 0);
			TAILQ_INSERT_TAIL(&detzone_ns->internal.zone[zone_idx].wr_waiting_cpl, io_ctx, link);
		}
		// try to submit write I/O for this zone
		_detzone_ns_io_write_submit(ch, detzone_ns, &detzone_ns->internal.zone[zone_idx]);
	} else {
		// write scheduler will submit pending IOs
		TAILQ_INSERT_TAIL(&detzone_ns->internal.zone[zone_idx].wr_pending_queue, io_ctx, link);
	}
}

static int
vbdev_detzone_ns_write_sched(void *arg)
{
	struct detzone_io_channel *detzone_ch = arg;
	struct spdk_io_channel *ch = spdk_io_channel_from_ctx(detzone_ch);
	struct vbdev_detzone_ns *detzone_ns = spdk_io_channel_get_io_device(ch);
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_ns_zone *zone, *tmp_zone;
	struct spdk_bdev_io *bdev_io;
	struct detzone_bdev_io *io_ctx;
	uint64_t now = spdk_get_ticks();
	int rc = 0;

	if (TAILQ_EMPTY(&detzone_ns->internal.zone_write_queue)) {
		return SPDK_POLLER_IDLE;
	}

	TAILQ_FOREACH_SAFE(zone, &detzone_ns->internal.zone_write_queue, link, tmp_zone) {
		// refill tokens
		zone->tb_tokens += (now - zone->tb_last_update_tsc) * detzone_ns->zone_stripe_width / detzone_ctrlr->blk_latency_thresh_ticks;
		zone->tb_tokens = spdk_min(zone->tb_tokens, detzone_ns->zone_stripe_tb_size * detzone_ns->zone_stripe_width);
		zone->tb_last_update_tsc = now;
		//SPDK_DEBUGLOG(vbdev_detzone, "tb refilled: %lu blks\n", zone->tb_tokens);
		
		while ((io_ctx = TAILQ_FIRST(&zone->wr_pending_queue)) != NULL) {
			bdev_io = spdk_bdev_io_from_ctx(io_ctx);
			rc = _detzone_ns_io_write_split(detzone_ns, bdev_io);
			if (rc == -EAGAIN) {
				break;
			} else {
				TAILQ_REMOVE(&zone->wr_pending_queue, io_ctx, link);
				TAILQ_INSERT_TAIL(&zone->wr_waiting_cpl, io_ctx, link);
			}
		}
		_detzone_ns_io_write_submit(ch, detzone_ns, zone);
	}
	return SPDK_POLLER_BUSY;
}

static int
_abort_queued_io(void *_head, struct spdk_bdev_io *bio_to_abort)
{
	TAILQ_HEAD(, detzone_bdev_io) *head = _head;
	struct detzone_bdev_io *io_ctx_to_abort = (struct detzone_bdev_io *)bio_to_abort->driver_ctx;
	struct detzone_bdev_io *io_ctx;

	TAILQ_FOREACH(io_ctx, head, link) {
		if (io_ctx == io_ctx_to_abort) {
			if (io_ctx->is_busy) {
				// We cannot abort I/O in-processing
				return -EBUSY;
			} else {
				// We can abort this I/O that has not yet submited any child commands
				spdk_bdev_io_complete(bio_to_abort, SPDK_BDEV_IO_STATUS_ABORTED);
				TAILQ_REMOVE(head, io_ctx, link);
				return 0;
			}
		}
	}
	return -ENOENT;
}

static void
_detzone_ns_reset_done(struct spdk_io_channel_iter *i, int status)
{
	struct spdk_bdev_io *bdev_io = spdk_io_channel_iter_get_ctx(i);

	//TODO: we may rescan physical zone info at the end of reset..
	spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
}

static void
_detzone_ns_reset_channel(struct spdk_io_channel_iter *i)
{
	//struct spdk_io_channel *ch = spdk_io_channel_iter_get_channel(i);
	//struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);

	//TODO: abort on-going I/Os if possible
	spdk_for_each_channel_continue(i, 0);
}

static int
_detzone_ns_abort(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	/*
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns,
					 detzone_ns_bdev);
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);
	struct spdk_bdev_io *bio_to_abort = bdev_io->u.abort.bio_to_abort;
	//struct detzone_bdev_io *io_ctx_to_abort = (struct detzone_bdev_io *)bio_to_abort->driver_ctx;

	return spdk_bdev_abort(detzone_ctrlr->base_desc, detzone_ch->base_ch, bio_to_abort,
			       _detzone_ns_complete_io, bdev_io);
	*/

	spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	return 0;
}

static int
_detzone_ns_get_zone_info(struct vbdev_detzone_ns *detzone_ns, uint64_t zslba,
							 uint32_t num_zones, struct spdk_bdev_zone_info *info)
{
	uint64_t zone_idx, num_lzones;
	uint32_t i;

	if (zslba % detzone_ns->detzone_ns_bdev.zone_size != 0) {
		return -EINVAL;
	}

	zone_idx = vbdev_detzone_ns_get_zone_idx(detzone_ns, zslba);
	num_lzones = detzone_ns->detzone_ns_bdev.blockcnt / detzone_ns->detzone_ns_bdev.zone_size;

	for (i=0; i < num_zones && zone_idx + i < num_lzones; i++) {
		info[i].zone_id = vbdev_detzone_ns_get_zone_id_by_idx(detzone_ns, zone_idx + i);;
		info[i].write_pointer = vbdev_detzone_ns_get_zone_wp(detzone_ns, info[i].zone_id);
		info[i].capacity = vbdev_detzone_ns_get_zone_cap(detzone_ns, info[i].zone_id);
		info[i].state = vbdev_detzone_ns_get_zone_state(detzone_ns, info[i].zone_id);
	}
	return 0;
}

static int
_detzone_ns_zone_management(struct vbdev_detzone_ns *detzone_ns, struct spdk_bdev_io *bdev_io,
							  uint64_t lzslba, bool sel_all, enum spdk_bdev_zone_action action,
							  detzone_ns_mgmt_completion_cb cb, void *cb_arg)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx;
	spdk_msg_fn msg_func;

	if (!sel_all) {
		if (lzslba % detzone_ns->detzone_ns_bdev.zone_size ||
				lzslba >= detzone_ns->detzone_ns_bdev.blockcnt) {
			return -EINVAL;
		}
	}
	mgmt_io_ctx = calloc(1, sizeof(struct vbdev_detzone_ns_mgmt_io_ctx));
	if (!mgmt_io_ctx) {
		return -ENOMEM;
	}
	mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	mgmt_io_ctx->zone_mgmt.zone_id = sel_all ? 0 : lzslba;
	mgmt_io_ctx->zone_mgmt.zone_action = action;
	mgmt_io_ctx->zone_mgmt.select_all = sel_all;
	mgmt_io_ctx->zone_mgmt.num_zones = sel_all ? detzone_ns->detzone_ns_bdev.blockcnt /
												 detzone_ns->detzone_ns_bdev.zone_size
										 : 1;
	mgmt_io_ctx->parent_io = bdev_io;
	mgmt_io_ctx->submited_thread = spdk_get_thread();
	mgmt_io_ctx->detzone_ns = detzone_ns;
	mgmt_io_ctx->remain_ios = mgmt_io_ctx->zone_mgmt.num_zones * detzone_ns->zone_stripe_width;
	mgmt_io_ctx->outstanding_mgmt_ios = 0;
	mgmt_io_ctx->cb = cb;
	mgmt_io_ctx->cb_arg = cb_arg;

	SPDK_DEBUGLOG(vbdev_detzone,
						 "Zone management: action(%u) select_all(%d) implicit(%d) ns_id(%u) logi_zone_id(0x%lx)\n",
						 action, sel_all, bdev_io->type == SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT ? 0:1, detzone_ns->nsid, lzslba);
	switch (action) {
	case SPDK_BDEV_ZONE_CLOSE:
		msg_func = vbdev_detzone_ns_close_zone;
		break;
	case SPDK_BDEV_ZONE_FINISH:
		msg_func = vbdev_detzone_ns_finish_zone;
		break;
	case SPDK_BDEV_ZONE_OPEN:
		if (bdev_io->type == SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT) {
			msg_func = vbdev_detzone_ns_open_zone;
		} else {
			msg_func = vbdev_detzone_ns_imp_open_zone;
		}
		break;
	case SPDK_BDEV_ZONE_RESET:
		msg_func = vbdev_detzone_ns_reset_zone;
		break;
	case SPDK_BDEV_ZONE_OFFLINE:
		msg_func = vbdev_detzone_ns_offline_zone;
		break;
	case SPDK_BDEV_ZONE_SET_ZDE:
	default:
		return -EINVAL;
	}

	if (detzone_ctrlr->thread == mgmt_io_ctx->submited_thread) {
		msg_func(mgmt_io_ctx);
		return 0;
	} else {
		return spdk_thread_send_msg(detzone_ctrlr->thread, msg_func, mgmt_io_ctx);
	}
}

/// Completion callback for mgmt commands that were issued from this bdev.
static void
_detzone_complete_mgmt(struct spdk_bdev_io *bdev_io, int sct, int sc, void *cb_arg)
{
	spdk_bdev_io_complete_nvme_status(bdev_io, 0, sct, sc);
	return;
}

static int
_detzone_ns_mgmt_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns, detzone_ns_bdev);
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	int rc = 0;

	io_ctx->type = DETZONE_IO_MGMT;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_GET_ZONE_INFO:
		rc = _detzone_ns_get_zone_info(detzone_ns, bdev_io->u.zone_mgmt.zone_id,
					 bdev_io->u.zone_mgmt.num_zones, bdev_io->u.zone_mgmt.buf);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
		break;
	case SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT:
		rc = _detzone_ns_zone_management(detzone_ns, bdev_io, bdev_io->u.zone_mgmt.zone_id,
										bdev_io->u.zone_mgmt.sel_all,
										bdev_io->u.zone_mgmt.zone_action,
										_detzone_complete_mgmt, NULL);
		break;
	default:
		rc = -EINVAL;
		SPDK_ERRLOG("detzone: unknown I/O type %d\n", bdev_io->type);
		break;
	}
	return rc;
}

static void
_vbdev_detzone_ns_submit_request_write(void *arg)
{
	struct spdk_bdev_io *bdev_io = arg;
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	struct vbdev_detzone_ns *detzone_ns = spdk_io_channel_get_io_device(spdk_bdev_io_get_io_channel(bdev_io));
	int rc = 0;

	io_ctx->ch = detzone_ns->wr_ch;
	io_ctx->u.io.iov_offset = 0;
	io_ctx->u.io.iov_idx = 0;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_ZONE_APPEND:
		io_ctx->type = DETZONE_IO_APPEND;
		io_ctx->u.io.remain_blocks = bdev_io->u.bdev.num_blocks;
		if (spdk_unlikely(bdev_io->u.bdev.offset_blocks % detzone_ns->detzone_ns_bdev.zone_size)) {
			io_ctx->nvme_status.sct = SPDK_NVME_SCT_GENERIC;
			io_ctx->nvme_status.sc = SPDK_NVME_SC_INVALID_FIELD;
			goto error_complete;
		} else {
			io_ctx->u.io.next_offset_blocks = vbdev_detzone_ns_get_zone_append_pointer(detzone_ns, bdev_io->u.bdev.offset_blocks);
			if (io_ctx->u.io.next_offset_blocks + bdev_io->u.bdev.num_blocks >
							bdev_io->u.bdev.offset_blocks +
							 vbdev_detzone_ns_get_zone_cap(detzone_ns, bdev_io->u.bdev.offset_blocks)) {
				io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
				io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_BOUNDARY_ERROR;
				goto error_complete;
			} else {
				SPDK_DEBUGLOG(vbdev_detzone, "ZONE_APPEND at the writepointer %lu (%lu blocks)\n",
															bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);
				bdev_io->u.bdev.offset_blocks = io_ctx->u.io.next_offset_blocks;
			}
		}
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		io_ctx->type = DETZONE_IO_WRITE;
		io_ctx->u.io.remain_blocks = bdev_io->u.bdev.num_blocks;
		io_ctx->u.io.next_offset_blocks = bdev_io->u.bdev.offset_blocks;
		if (bdev_io->u.bdev.offset_blocks !=
							vbdev_detzone_ns_get_zone_wp(detzone_ns, bdev_io->u.bdev.offset_blocks)) {
			SPDK_ERRLOG("Invalid WP given: request_wp(0x%lx) curr_wp(0x%lx) zs(%u)\n",
									bdev_io->u.bdev.offset_blocks,
									vbdev_detzone_ns_get_zone_wp(detzone_ns, bdev_io->u.bdev.offset_blocks),
									vbdev_detzone_ns_get_zone_state(detzone_ns, bdev_io->u.bdev.offset_blocks));
			io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_INVALID_WRITE;
			goto error_complete;
		}
		break;
	default:
		assert(0);
		io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
		goto error_complete;
	}

	switch (vbdev_detzone_ns_get_zone_state(detzone_ns, io_ctx->u.io.next_offset_blocks)) {
	case SPDK_BDEV_ZONE_STATE_FULL:
		io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
		io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_IS_FULL;
		goto error_complete;
	case SPDK_BDEV_ZONE_STATE_READ_ONLY:
		io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
		io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_IS_READONLY;
		goto error_complete;
	case SPDK_BDEV_ZONE_STATE_OFFLINE:
		io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
		io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_IS_OFFLINE;
		goto error_complete;
	case SPDK_BDEV_ZONE_STATE_CLOSED:
		rc = _detzone_ns_zone_management(detzone_ns, bdev_io,
										 vbdev_detzone_ns_get_zone_id(detzone_ns, 
																bdev_io->u.bdev.offset_blocks),
										false,
										SPDK_BDEV_ZONE_OPEN, _detzone_ns_write_cb, io_ctx->ch);
		break;
	case SPDK_BDEV_ZONE_STATE_EMPTY:
		rc = _detzone_ns_zone_management(detzone_ns, bdev_io, bdev_io->u.bdev.offset_blocks, false,
										SPDK_BDEV_ZONE_OPEN, _detzone_ns_write_cb, io_ctx->ch);
		break;
	default:
		if ((bdev_io->u.bdev.offset_blocks % detzone_ns->detzone_ns_bdev.zone_size) +
						bdev_io->u.bdev.num_blocks > detzone_ns->zcap) {
			io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_BOUNDARY_ERROR;
			goto error_complete;
		} else {
			_detzone_ns_write_cb(bdev_io, 0, 0, io_ctx->ch);
		}
		break;
	}

	if (rc == -ENOMEM) {
		SPDK_ERRLOG("No memory, start to queue io for detzone.\n");
		vbdev_detzone_queue_io(bdev_io);
	} else if (rc != 0) {
		SPDK_ERRLOG("ERROR on bdev_io submission!\n");
		io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
		goto error_complete;
	}

	return;

error_complete:
	if (spdk_bdev_io_get_thread(bdev_io) == detzone_ns->wr_thread) {
		_detzone_ns_io_complete(io_ctx);
	} else {
		spdk_thread_send_msg(spdk_bdev_io_get_thread(bdev_io), _detzone_ns_io_complete, io_ctx);
	}
}

static void
vbdev_detzone_ns_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns, detzone_ns_bdev);
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	int rc = 0;

	io_ctx->status = SPDK_BDEV_IO_STATUS_PENDING;

	//SPDK_DEBUGLOG(vbdev_detzone, "submit_request: type(%u)\n", bdev_io->type);
	
	switch (bdev_io->type) {
	// Try to abort I/O if it is a R/W I/O in congestion queues or management command.
	// We cannot abort R/W I/Os already in progress because we may split them.
	case SPDK_BDEV_IO_TYPE_ABORT:
		rc = _detzone_ns_abort(ch, bdev_io);
		break;

	// TODO: We need a special handling for ZONE_OPEN/CLOSE for striped zones.
	case SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT:
	case SPDK_BDEV_IO_TYPE_GET_ZONE_INFO:
		rc = _detzone_ns_mgmt_submit_request(ch, bdev_io);
		break;

	case SPDK_BDEV_IO_TYPE_FLUSH:
		// TODO: We may flush I/O schueduler queues for a detzone_ns when FLUSH is submitted.
		// FLUSH may be suspended until all I/O commands in queues complete to emulate FLUSH operation.
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
		break;

	case SPDK_BDEV_IO_TYPE_RESET:
		spdk_for_each_channel(detzone_ns, _detzone_ns_reset_channel, bdev_io,
				      _detzone_ns_reset_done);
		break;

	case SPDK_BDEV_IO_TYPE_READ:
		io_ctx->ch = ch;
		io_ctx->type = DETZONE_IO_READ;
		io_ctx->u.io.iov_offset = 0;
		io_ctx->u.io.iov_idx = 0;
		io_ctx->u.io.remain_blocks = bdev_io->u.bdev.num_blocks;
		io_ctx->u.io.next_offset_blocks = bdev_io->u.bdev.offset_blocks;
		spdk_bdev_io_get_buf(bdev_io, _detzone_ns_read_get_buf_cb,
					bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		break;

	case SPDK_BDEV_IO_TYPE_ZONE_APPEND:
	case SPDK_BDEV_IO_TYPE_WRITE:
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		if (detzone_ns->wr_thread == spdk_get_thread()) {
			_vbdev_detzone_ns_submit_request_write(bdev_io);
		} else {
			spdk_thread_send_msg(detzone_ns->wr_thread, _vbdev_detzone_ns_submit_request_write, bdev_io);
		}
		break;
	default:
		rc = -ENOTSUP;
		break;
	}

	if (rc == -ENOMEM) {
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

	return spdk_bdev_io_type_supported(detzone_ctrlr->base_bdev, io_type);
}

static bool
vbdev_detzone_ns_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	//struct vbdev_detzone_ns *detzone_ns = (struct vbdev_detzone_ns *)ctx;
	//struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;

	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_READ:
	case SPDK_BDEV_IO_TYPE_WRITE:
	case SPDK_BDEV_IO_TYPE_FLUSH:
	case SPDK_BDEV_IO_TYPE_RESET:
	case SPDK_BDEV_IO_TYPE_GET_ZONE_INFO:
	case SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT:
	case SPDK_BDEV_IO_TYPE_ZONE_APPEND:
		return true;
	case SPDK_BDEV_IO_TYPE_ZCOPY:
	case SPDK_BDEV_IO_TYPE_NVME_ADMIN:
	case SPDK_BDEV_IO_TYPE_NVME_IO:
	case SPDK_BDEV_IO_TYPE_NVME_IO_MD:
	case SPDK_BDEV_IO_TYPE_COMPARE:
	case SPDK_BDEV_IO_TYPE_COMPARE_AND_WRITE:
	case SPDK_BDEV_IO_TYPE_UNMAP:
	default:
		return false;
		//return spdk_bdev_io_type_supported(detzone_ctrlr->base_bdev, io_type);
	}
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
	spdk_json_write_named_uint32(w, "zone_stripe_width", detzone_ns->zone_stripe_width);
	spdk_json_write_named_uint32(w, "stripe_size", detzone_ns->zone_stripe_blks);
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

	detzone_ch->base_ch = spdk_bdev_get_io_channel(detzone_ctrlr->base_desc);
	detzone_ch->ch_id = ++detzone_ns->ref;

	SPDK_DEBUGLOG(vbdev_detzone, "detzone ns: create io channel %u (is_writable: %d)\n", detzone_ch->ch_id, detzone_ns->wr_thread == NULL);
	if (detzone_ns->wr_thread == NULL) {
		detzone_ns->wr_thread = spdk_get_thread();
		detzone_ns->wr_ch = spdk_io_channel_from_ctx(detzone_ch);
		detzone_ch->write_sched_poller = SPDK_POLLER_REGISTER(vbdev_detzone_ns_write_sched, detzone_ch, 1000);
	}

	return 0;
}

static int
detzone_bdev_mgmt_ch_create_cb(void *io_device, void *ctx_buf)
{
	//struct detzone_mgmt_channel *detzone_mgmt_ch = ctx_buf;
	//struct vbdev_detzone *detzone_ctrlr = io_device;

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
	struct vbdev_detzone_ns *detzone_ns = io_device;
	--detzone_ns->ref;
	if (detzone_ns->wr_thread != NULL) {
		detzone_ns->wr_thread = NULL;
		detzone_ns->wr_ch = NULL;
		spdk_poller_unregister(&detzone_ch->write_sched_poller);
		detzone_ch->write_sched_poller = NULL;
	}
	spdk_put_io_channel(detzone_ch->base_ch);
}

static void
detzone_bdev_mgmt_ch_destroy_cb(void *io_device, void *ctx_buf)
{
	//struct detzone_mgmt_channel *detzone_mgmt_ch = ctx_buf;
	//struct vbdev_detzone *detzone_ctrlr = io_device;
}

/* Create the detzone association from the bdev and vbdev name and insert
 * on the global list. */
static int
vbdev_detzone_ns_insert_association(const char *detzone_name, const char *ns_name,
					uint32_t zone_stripe_width, uint32_t stripe_size, uint32_t block_align,
					uint64_t num_base_zones)
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
		assoc->zone_stripe_width = zone_stripe_width;
		assoc->stripe_size = stripe_size;
		assoc->block_align = block_align;
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
	// Init mempool for management commands
	g_detzone_mgmt_buf_pool = spdk_mempool_create("detzone_mgmt_buf_pool",
					   1024,
					   1024,
					   SPDK_MEMPOOL_DEFAULT_CACHE_SIZE,
					   SPDK_ENV_SOCKET_ID_ANY);
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

	spdk_mempool_free(g_detzone_mgmt_buf_pool);
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
	free(detzone_ns->internal.zone);
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

static void
_dump_zone_info(struct vbdev_detzone_ns *detzone_ns)
{
	struct vbdev_detzone_ns_zone *zone = &detzone_ns->internal.zone[0];
	uint64_t total_lzones, i, j;

	total_lzones = detzone_ns->detzone_ns_bdev.blockcnt / detzone_ns->detzone_ns_bdev.zone_size;
	fprintf(stderr, "--------------------------------\n");
	fprintf(stderr, "NS ID: %u\nZONE SIZE: %lu\nZONE CAPACITY: %lu\nNUM ZONES: %lu\nSTRIPE WIDTH: %u\nSTRIPE SIZE: %u\n",
						detzone_ns->nsid, detzone_ns->detzone_ns_bdev.zone_size,
						detzone_ns->zcap, total_lzones, detzone_ns->zone_stripe_width, detzone_ns->zone_stripe_blks);
	for (i=0; i < total_lzones; i++) {
		fprintf(stderr, "- {zslba: 0x%010lx, wp: 0x%010lx, zcap: %lu, state: 0x%x",
					zone[i].zone_id,
					zone[i].write_pointer,
					zone[i].capacity,
					zone[i].state);
		for (j=0; j < detzone_ns->zone_stripe_width; j++) {
			fprintf(stderr, ", bz_id[%lu]: 0x%010lx", j, zone[i].base_zone[j].zone_id);
		}
		fprintf(stderr, "}\n");
	}
}

static int
vbdev_detzone_ns_register(const char *ctrl_name, const char *ns_name)
{
	struct ns_association *assoc;
	struct vbdev_detzone *detzone_ctrlr;
	struct vbdev_detzone_ns *detzone_ns = NULL;
	struct vbdev_detzone_ns_zone *zone;
	struct vbdev_detzone_zone_info *phy_zone_info;
	struct spdk_bdev *bdev;
	uint64_t total_lzones, i, zone_idx, base_zone_idx;
	uint64_t valid_blks, last_stripes_blks;
	uint32_t j;
	int rc = 0;

	assert(ctrl_name && ns_name);

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

			total_lzones = assoc->zone_stripe_width ? assoc->num_base_zones / assoc->zone_stripe_width : assoc->num_base_zones;
			if (total_lzones == 0) {
				SPDK_ERRLOG("could not create zero sized detzone namespace\n");
				return -EINVAL;
			}
			detzone_ns = calloc(1, sizeof(struct vbdev_detzone_ns));
			if (!detzone_ns) {
				SPDK_ERRLOG("could not allocate detzone_ns\n");
				return -ENOMEM;
			}
			detzone_ns->internal.zone = calloc(total_lzones, sizeof(struct vbdev_detzone_ns_zone));
			if (!detzone_ns->internal.zone) {
				SPDK_ERRLOG("could not allocate detzone_ns zone info\n");
				free(detzone_ns);
				return -ENOMEM;
			}
			detzone_ns->detzone_ns_bdev.name = strdup(assoc->ns_name);
			if (!detzone_ns->detzone_ns_bdev.name) {
				SPDK_ERRLOG("could not allocate detzone_bdev name\n");
				free(detzone_ns->internal.zone);
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
			detzone_ns->zone_stripe_blks = assoc->stripe_size ? (assoc->stripe_size / bdev->blocklen) : bdev->optimal_io_boundary;
			if (detzone_ns->zone_stripe_blks == 0) {
				detzone_ns->zone_stripe_blks = 1;
			}
			if (bdev->zone_size % detzone_ns->zone_stripe_blks) {
				rc = -EINVAL;
				SPDK_ERRLOG("base bdev zone size must be stripe size aligned\n");
				goto error_close;
			}
			detzone_ns->block_align = assoc->block_align;

			detzone_ns->detzone_ns_bdev.write_cache = bdev->write_cache;
			detzone_ns->detzone_ns_bdev.optimal_io_boundary = bdev->optimal_io_boundary;

			detzone_ns->base_zone_size = bdev->zone_size;
			// Configure namespace specific parameters
			detzone_ns->zone_stripe_width = assoc->zone_stripe_width ? assoc->zone_stripe_width : 1;
			detzone_ns->zone_stripe_tb_size = detzone_ctrlr->per_zone_mdts;

			// Caculate the number of padding blocks (reserve block + meta block + padding)
			// Padding to align the end of base zone to stripe end
			detzone_ns->padding_blocks = DETZONE_RESERVATION_BLKS + DETZONE_INLINE_META_BLKS +
											 (detzone_ns->base_zone_size - (DETZONE_RESERVATION_BLKS +
											 DETZONE_INLINE_META_BLKS)) % detzone_ns->zone_stripe_blks;
			//TODO: should check base_bdev zone capacity
			detzone_ns->zcap = detzone_ns->base_zone_size * detzone_ns->zone_stripe_width
								 - detzone_ns->padding_blocks * detzone_ns->zone_stripe_width;

			detzone_ns->detzone_ns_bdev.zone_size = spdk_align64pow2(detzone_ns->zcap);
			//detzone_ns->detzone_ns_bdev.zone_size = detzone_ns->zcap;
			detzone_ns->detzone_ns_bdev.required_alignment = bdev->required_alignment;
			detzone_ns->detzone_ns_bdev.max_zone_append_size = bdev->max_zone_append_size;
			detzone_ns->detzone_ns_bdev.max_open_zones = spdk_max(1, 192/detzone_ns->zone_stripe_width);
			detzone_ns->detzone_ns_bdev.max_active_zones = spdk_max(1, 192/detzone_ns->zone_stripe_width);
			detzone_ns->detzone_ns_bdev.optimal_open_zones = spdk_max(1, 192/detzone_ns->zone_stripe_width);
		
			detzone_ns->detzone_ns_bdev.blocklen = bdev->blocklen;
			// TODO: support configurable block length (blocklen)
			//detzone_ns->detzone_ns_bdev.phys_blocklen = bdev->blocklen;

			detzone_ns->detzone_ns_bdev.blockcnt = total_lzones * detzone_ns->detzone_ns_bdev.zone_size;

			detzone_ns->internal.epoch_num_pu = 0;
			detzone_ns->internal.epoch_pu_map = spdk_bit_array_create(detzone_ctrlr->num_pu);
			if (!detzone_ns->internal.epoch_pu_map) {
				SPDK_ERRLOG("cannot create a PU Map\n");
				rc = -ENOMEM;
				goto error_close;
			}
			TAILQ_INIT(&detzone_ns->internal.zone_write_queue);

			// it looks dumb... but let just keep it...
			assert(!SPDK_BDEV_ZONE_STATE_EMPTY);
			zone = detzone_ns->internal.zone;
			phy_zone_info = detzone_ctrlr->zone_info;
			// Init zone info (set non-zero init values)
			for (i=0; i < total_lzones; i++) {
				zone[i].capacity = detzone_ns->zcap;
				zone[i].zone_id = detzone_ns->detzone_ns_bdev.zone_size * i;
				zone[i].write_pointer = zone[i].zone_id;
				TAILQ_INIT(&zone[i].wr_pending_queue);
				TAILQ_INIT(&zone[i].wr_waiting_cpl);
				for (j=0; j < detzone_ns->zone_stripe_width; j++) {
					zone[i].base_zone[j].zone_id = UINT64_MAX;
				}
			}
			// Preload existing zone info
			for (i=DETZONE_RESERVED_ZONES; i < detzone_ctrlr->num_zones; i++) {
				if (phy_zone_info[i].ns_id != detzone_ns->nsid) {
					continue;
				}
				if (phy_zone_info[i].stripe_width != detzone_ns->zone_stripe_width ||
						phy_zone_info[i].stripe_size != detzone_ns->zone_stripe_blks) {
					SPDK_ERRLOG("Stripe metadata does not match\n");
					rc = -EINVAL;
					goto error_close;
				}
				zone_idx = phy_zone_info[i].lzone_id / detzone_ns->detzone_ns_bdev.zone_size;
				zone[zone_idx].base_zone[phy_zone_info[i].stripe_id].zone_id = phy_zone_info[i].zone_id;
				// TODO: if any zone has a partially written stripe, we have to recover.
				// we may copy valid data to another physical zones and discard the partial write. 
				if (phy_zone_info[i].state == SPDK_BDEV_ZONE_STATE_FULL) {
					zone[zone_idx].write_pointer += phy_zone_info[i].capacity - detzone_ns->padding_blocks;
				} else {
					zone[zone_idx].write_pointer += phy_zone_info[i].write_pointer - (
													phy_zone_info[i].zone_id + detzone_ns->padding_blocks);
				}
				zone[zone_idx].state = SPDK_BDEV_ZONE_STATE_CLOSED;
			}
			// Parse existings, and initialize current zone info
			for (i=0; i < total_lzones; i++) {
				switch (zone[i].state) {
				case SPDK_BDEV_ZONE_STATE_EMPTY:
					break;

				case SPDK_BDEV_ZONE_STATE_CLOSED:
					if (zone[i].write_pointer == zone[i].zone_id + zone[i].capacity) {
						for (j=0; j < detzone_ns->zone_stripe_width; j++) {
							// TODO: we may need to validate these as well
							base_zone_idx = zone[i].base_zone[j].zone_id / detzone_ns->base_zone_size;
							if (phy_zone_info[base_zone_idx].state != SPDK_BDEV_ZONE_STATE_FULL) {
								SPDK_ERRLOG("Basezone state does not match\n");
								rc = -EINVAL;
								goto error_close;								
							}
						}
						//zone[i].write_pointer = zone[i].zone_id;
						zone[i].write_pointer = zone[i].zone_id + zone[i].capacity;
						zone[i].state = SPDK_BDEV_ZONE_STATE_FULL;
						break;
					} else {
						// Validate the zone write pointer
						for (j=0; j < detzone_ns->zone_stripe_width; j++) {
							if (zone[i].base_zone[j].zone_id == UINT64_MAX) {
								SPDK_ERRLOG("Broken zone\n");
								rc = -EINVAL;
								goto error_close;								
							}
							base_zone_idx = zone[i].base_zone[j].zone_id / detzone_ns->base_zone_size;
							// calculate the number of full stripes excluding the last stripe
							valid_blks = (zone[i].write_pointer - zone[i].zone_id) /
											 (detzone_ns->zone_stripe_blks * detzone_ns->zone_stripe_width) *
											 detzone_ns->zone_stripe_blks;
							// calculate the total bytes of last stipes
							last_stripes_blks = (zone[i].write_pointer - zone[i].zone_id)
													 - (valid_blks * detzone_ns->zone_stripe_width);
							if ((j + 1) * detzone_ns->zone_stripe_blks <= last_stripes_blks) {
								// the last one is a full stripe
								valid_blks += detzone_ns->zone_stripe_blks;
							} else if (j * detzone_ns->zone_stripe_blks <= last_stripes_blks) {
								// the last one is partially filled stripe
								valid_blks += last_stripes_blks % detzone_ns->zone_stripe_blks;
							} // else, the last one is an empty stripe

							if (((phy_zone_info[base_zone_idx].state == SPDK_BDEV_ZONE_STATE_FULL) &&
									(valid_blks == phy_zone_info[base_zone_idx].capacity)) || 
									(phy_zone_info[base_zone_idx].write_pointer -
											 phy_zone_info[base_zone_idx].zone_id - detzone_ns->padding_blocks == valid_blks)) {
								// this physical zone is valid
								break;
							}
							SPDK_ERRLOG("Broken stripe! (lzone_wp:0x%lx) valid_blks:0x%lx pad:0x%x state:%u lzone_id:0x%lx zone_id:0x%lx zone_wp:0x%lx cap:0x%lx\n",
												zone[i].write_pointer, valid_blks, detzone_ns->padding_blocks,
												phy_zone_info[base_zone_idx].state,
												phy_zone_info[base_zone_idx].lzone_id,
												phy_zone_info[base_zone_idx].zone_id,
												phy_zone_info[base_zone_idx].write_pointer,
												phy_zone_info[base_zone_idx].capacity);
							rc = -EINVAL;
							goto error_close;								
						}
						detzone_ns->num_active_zones++;
					}

					break;

				default:
					// This case is not possible
					assert(0);
					break;
				}
			}

			//_dump_zone_info(detzone_ns);

			spdk_io_device_register(detzone_ns, detzone_bdev_io_ch_create_cb, detzone_bdev_io_ch_destroy_cb,
						sizeof(struct detzone_io_channel),
						assoc->ns_name);
			
			rc = spdk_bdev_register(&detzone_ns->detzone_ns_bdev);
			if (rc) {
				SPDK_ERRLOG("could not register detzone_ns_bdev\n");
				spdk_io_device_unregister(detzone_ns, NULL);
				goto error_close;
			}

			++detzone_ctrlr->num_ns;
			TAILQ_INSERT_TAIL(&detzone_ctrlr->ns, detzone_ns, link);
		}
	}

	return 0;

error_close:
	free(detzone_ns->detzone_ns_bdev.name);
	spdk_bit_array_free(&detzone_ns->internal.epoch_pu_map);
	free(detzone_ns->internal.zone);
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
	//struct spdk_bdev_desc *desc = ctx;
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)ctx;

	spdk_put_io_channel(detzone_ctrlr->mgmt_ch);
	spdk_poller_unregister(&detzone_ctrlr->mgmt_poller);
	spdk_bdev_close(detzone_ctrlr->base_desc);
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
		spdk_thread_send_msg(detzone_ctrlr->thread, _vbdev_detzone_destruct_cb, detzone_ctrlr);
	} else {
		_vbdev_detzone_destruct_cb(detzone_ctrlr);
	}

	/* Unregister the io_device. */
	spdk_io_device_unregister(detzone_ctrlr, _device_unregister_cb);

	return 0;
}

/* We currently don't support a normal I/O command in detzone_mgmt bdev.
 *  detzone_mgmt is only used for internal management and creating virtual namespace.
 */
static void
vbdev_detzone_mgmt_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	//SPDK_ERRLOG("detzone: mgmt does not support a normal I/O type %d\n", bdev_io->type);
	spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	return;
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
_vbdev_detzone_wr_stat_iter(struct spdk_io_channel_iter *i)
{
	//struct vbdev_detzone_ns *detzone_ns = spdk_io_channel_iter_get_io_device(i);
	struct vbdev_detzone *detzone_ctrlr = spdk_io_channel_iter_get_ctx(i);
	struct spdk_io_channel *ch = spdk_io_channel_iter_get_channel(i);
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);

	if (detzone_ch->write_blks) {
		detzone_ctrlr->internal.active_channels++;
		detzone_ctrlr->internal.total_write_blk_tsc += detzone_ch->total_write_blk_tsc
														/ detzone_ch->write_blks;
		detzone_ch->write_blks = 0;
		detzone_ch->total_write_blk_tsc = 0;
	}

	spdk_for_each_channel_continue(i, 0);
}

static void
_vbdev_detzone_wr_stat_done(struct spdk_io_channel_iter *i, int status)
{
	struct vbdev_detzone_ns *detzone_ns = spdk_io_channel_iter_get_io_device(i);
	struct vbdev_detzone *detzone_ctrlr = spdk_io_channel_iter_get_ctx(i);
	struct vbdev_detzone_ns *detzone_ns_next;
	uint64_t	avg_write_lat_tsc;

	assert(spdk_get_thread() == detzone_ctrlr->thread);

	detzone_ns_next = TAILQ_NEXT(detzone_ns, link);
	if (detzone_ns_next == NULL) {
		if (detzone_ctrlr->internal.active_channels) {
			avg_write_lat_tsc = detzone_ctrlr->internal.total_write_blk_tsc
														/ detzone_ctrlr->internal.active_channels;
			if (1) {
				uint64_t ticks_hz = spdk_get_ticks_hz();
				printf("Update lat thresh: (avg blk lat: %llu us) %llu us --> %llu us \n",
						avg_write_lat_tsc * SPDK_SEC_TO_USEC / ticks_hz,
						detzone_ctrlr->blk_latency_thresh_ticks * SPDK_SEC_TO_USEC / ticks_hz,
						((avg_write_lat_tsc + detzone_ctrlr->blk_latency_thresh_ticks) >> 1) * SPDK_SEC_TO_USEC / ticks_hz);

			}
			detzone_ctrlr->blk_latency_thresh_ticks =
								(avg_write_lat_tsc + detzone_ctrlr->blk_latency_thresh_ticks) >> 1;
			detzone_ctrlr->internal.active_channels = 0;
			detzone_ctrlr->internal.total_write_blk_tsc = 0;
		}
		
		spdk_poller_resume(detzone_ctrlr->mgmt_poller);
	} else {
		spdk_for_each_channel(detzone_ns_next,
								 _vbdev_detzone_wr_stat_iter,
								 spdk_io_channel_iter_get_ctx(i),
								 _vbdev_detzone_wr_stat_done);
	}
}

static int
vbdev_detzone_poller_wr_sched(void *arg)
{
	struct vbdev_detzone *detzone_ctrlr = arg;
	struct vbdev_detzone_ns *detzone_ns;

	detzone_ns = TAILQ_FIRST(&detzone_ctrlr->ns);
	if (detzone_ns == NULL) {
		return SPDK_POLLER_IDLE;
	}

	detzone_ctrlr->internal.active_channels = 0;
	detzone_ctrlr->internal.total_write_blk_tsc = 0;

	spdk_poller_pause(detzone_ctrlr->mgmt_poller);
	spdk_for_each_channel(detzone_ns,
							 _vbdev_detzone_wr_stat_iter,
							 detzone_ctrlr,
							 _vbdev_detzone_wr_stat_done);
	return SPDK_POLLER_BUSY;
}

static void
_vbdev_detzone_fini_register(struct vbdev_detzone *detzone_ctrlr, struct vbdev_detzone_register_ctx *ctx)
{
	struct ns_association *assoc, *tmp_assoc;
	int rc;

	// TODO: we just use the current json config now.
	// but should read namespace metadata from persistent storage and compare with json.
	TAILQ_FOREACH_SAFE(assoc, &g_ns_associations, link, tmp_assoc) {
		if (strcmp(assoc->ns_name, detzone_ctrlr->mgmt_bdev.name) == 0) {
			rc = vbdev_detzone_ns_register(detzone_ctrlr->mgmt_bdev.name, assoc->ns_name);
			if (rc) {
				SPDK_ERRLOG("Unable to create ns %s on the detzone bdev %s. Removing the config entry\n",
								 assoc->ns_name, detzone_ctrlr->mgmt_bdev.name);
				TAILQ_REMOVE(&g_ns_associations, assoc, link);
				free(assoc->ctrl_name);
				free(assoc->ns_name);
				free(assoc);
			}
		}
	}

	vbdev_detzone_reserve_zone(detzone_ctrlr);
	TAILQ_INSERT_TAIL(&g_detzone_ctrlrs, detzone_ctrlr, link);
	detzone_ctrlr->mgmt_poller = SPDK_POLLER_REGISTER(vbdev_detzone_poller_wr_sched, detzone_ctrlr, 1000000);
	if (ctx->cb) {
		ctx->cb(ctx->cb_arg, 0);
	}
	free(ctx);
}

static void
_vbdev_detzone_init_zone_md_cb(void *cb_arg, bool success)
{
	struct vbdev_detzone_register_ctx *ctx = cb_arg;
	struct vbdev_detzone *detzone_ctrlr = ctx->detzone_ctrlr;
	uint64_t zone_idx;

	if (!success) {
		SPDK_ERRLOG("cannot retrieve the physical zone info for %s\n", detzone_ctrlr->mgmt_bdev.name);
		spdk_bdev_module_release_bdev(detzone_ctrlr->base_bdev);
		spdk_bdev_close(detzone_ctrlr->base_desc);
		spdk_io_device_unregister(detzone_ctrlr, NULL);
		free(detzone_ctrlr->zone_info);
		free(detzone_ctrlr->mgmt_bdev.name);
		free(detzone_ctrlr);

		if (ctx->cb) {
			ctx->cb(ctx->cb_arg, -EINVAL);
		}
		free(ctx->zone_md);
		free(ctx->ext_info);
		free(ctx);
		return;
	}

	// We don't use the first zone (zone_id == 0) for future use.
	// Also, it makes the zone validation easy as no allocated zone has zone_id 0
	for (zone_idx = DETZONE_RESERVED_ZONES; zone_idx < detzone_ctrlr->num_zones; zone_idx++)
	{
		detzone_ctrlr->zone_info[zone_idx].state = ctx->ext_info[zone_idx].state;
		detzone_ctrlr->zone_info[zone_idx].write_pointer = ctx->ext_info[zone_idx].write_pointer;
		detzone_ctrlr->zone_info[zone_idx].zone_id = ctx->ext_info[zone_idx].zone_id;
		detzone_ctrlr->zone_info[zone_idx].capacity = ctx->ext_info[zone_idx].capacity;
		detzone_ctrlr->zone_info[zone_idx].pu_group = detzone_ctrlr->num_pu;
		detzone_ctrlr->zone_info[zone_idx].ns_id = 0;
		detzone_ctrlr->zone_info[zone_idx].lzone_id = 0;
		detzone_ctrlr->zone_info[zone_idx].stripe_id = 0;
		detzone_ctrlr->zone_info[zone_idx].stripe_width = 0;
		detzone_ctrlr->zone_info[zone_idx].stripe_size = 0;
		//SPDK_DEBUGLOG(vbdev_detzone, "zone_id: 0x%lx  wp: 0x%lx state: %u\n", detzone_ctrlr->zone_info[zone_idx].zone_id,
		//				ctx->ext_info[zone_idx].write_pointer, detzone_ctrlr->zone_info[zone_idx].state);

		switch (ctx->ext_info[zone_idx].state) {
		case SPDK_BDEV_ZONE_STATE_EMPTY:
			TAILQ_INSERT_TAIL(&detzone_ctrlr->zone_empty,
								 &detzone_ctrlr->zone_info[zone_idx], link);
			detzone_ctrlr->num_zone_empty++;
			break;
		case SPDK_BDEV_ZONE_STATE_CLOSED:
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
			switch (ctx->ext_info[zone_idx].write_pointer -
							 ctx->ext_info[zone_idx].zone_id) {
			case 0:
				assert(0);
				// TODO: reset this zone and put it back to empty group
				TAILQ_INSERT_TAIL(&detzone_ctrlr->zone_empty,
									&detzone_ctrlr->zone_info[zone_idx], link);
				detzone_ctrlr->num_zone_empty++;
				break;
			case DETZONE_RESERVATION_BLKS:
				TAILQ_INSERT_TAIL(&detzone_ctrlr->zone_reserved,
									&detzone_ctrlr->zone_info[zone_idx], link);
				detzone_ctrlr->num_zone_reserved++;
				detzone_ctrlr->zone_alloc_cnt++;
				// this is meaningless number, but enough to run the algorithm.
				// in the future, we may store the PU group id at the reservation time.
				detzone_ctrlr->zone_info[zone_idx].pu_group = 
								(detzone_ctrlr->zone_alloc_cnt + 1) % detzone_ctrlr->num_pu;
				break;
			default:
				if (ctx->ext_info[zone_idx].write_pointer - ctx->ext_info[zone_idx].zone_id <
						DETZONE_RESERVATION_BLKS + DETZONE_INLINE_META_BLKS) {
					// TODO : this is undefined zone, may need reset
					break;
				}
				//spdk_bdev_read_blocks(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch, )
				detzone_ctrlr->zone_info[zone_idx].ns_id = ctx->zone_md[zone_idx].ns_id;
				detzone_ctrlr->zone_info[zone_idx].lzone_id = ctx->zone_md[zone_idx].lzone_id;
				detzone_ctrlr->zone_info[zone_idx].stripe_id = ctx->zone_md[zone_idx].stripe_id;
				detzone_ctrlr->zone_info[zone_idx].stripe_width = ctx->zone_md[zone_idx].stripe_width;
				detzone_ctrlr->zone_info[zone_idx].stripe_size = ctx->zone_md[zone_idx].stripe_size;
				
				// this means nothing, but just increase the alloc counter
				detzone_ctrlr->zone_alloc_cnt++;
			}
			break;
		case SPDK_BDEV_ZONE_STATE_FULL:
				detzone_ctrlr->zone_info[zone_idx].ns_id = ctx->zone_md[zone_idx].ns_id;
				detzone_ctrlr->zone_info[zone_idx].lzone_id = ctx->zone_md[zone_idx].lzone_id;
				detzone_ctrlr->zone_info[zone_idx].stripe_id = ctx->zone_md[zone_idx].stripe_id;
				detzone_ctrlr->zone_info[zone_idx].stripe_width = ctx->zone_md[zone_idx].stripe_width;
				detzone_ctrlr->zone_info[zone_idx].stripe_size = ctx->zone_md[zone_idx].stripe_size;
				
				// this means nothing, but just increase the alloc counter
				detzone_ctrlr->zone_alloc_cnt++;	
				break;
		default:
			// TODO: handle failed zones...
			assert(0);
			break;
		}
	}
	free(ctx->zone_md);
	free(ctx->ext_info);
	_vbdev_detzone_fini_register(detzone_ctrlr, ctx);
}

static void _vbdev_detzone_init_zone_info_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone_register_ctx *ctx = cb_arg;
	struct vbdev_detzone *detzone_ctrlr = ctx->detzone_ctrlr;

	spdk_bdev_free_io(bdev_io);
	if (!success) {
		SPDK_ERRLOG("cannot retrieve the physical zone info for %s\n", detzone_ctrlr->mgmt_bdev.name);
		spdk_bdev_module_release_bdev(detzone_ctrlr->base_bdev);
		spdk_bdev_close(detzone_ctrlr->base_desc);
		spdk_io_device_unregister(detzone_ctrlr, NULL);
		free(detzone_ctrlr->zone_info);
		free(detzone_ctrlr->mgmt_bdev.name);
		free(detzone_ctrlr);

		if (ctx->cb) {
			ctx->cb(ctx->cb_arg, -EINVAL);
		}
		free(ctx->ext_info);
		free(ctx);
		return;
	}

	vbdev_detzone_md_read(detzone_ctrlr, ctx->zone_md, 0,
							detzone_ctrlr->num_zones, _vbdev_detzone_init_zone_md_cb, ctx);
}

/* Create and register the detzone vbdev if we find it in our list of bdev names.
 * This can be called either by the examine path or RPC method.
 */
static int
vbdev_detzone_register(const char *bdev_name, vbdev_detzone_register_cb cb, void *cb_arg)
{
	struct bdev_association *assoc;
	struct vbdev_detzone *detzone_ctrlr;
	struct spdk_bdev *bdev;
	struct vbdev_detzone_register_ctx *ctx;
	int rc = -ENODEV;

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

		TAILQ_INIT(&detzone_ctrlr->zone_reserved);
		TAILQ_INIT(&detzone_ctrlr->zone_empty);

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
		if (!spdk_bdev_is_zoned(bdev) || strcmp(spdk_bdev_get_module_name(bdev), "nvme")) {
			rc = -EINVAL;
			SPDK_ERRLOG("detzone does not support non-zoned or non-nvme devices: %s\n", spdk_bdev_get_module_name(bdev));
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

		/* I/O scheduler specific parameters */
		detzone_ctrlr->blk_latency_thresh_ticks = spdk_get_ticks_hz() * 100 / SPDK_SEC_TO_USEC;
		detzone_ctrlr->per_zone_mdts = (128*1024UL) / detzone_ctrlr->mgmt_bdev.blocklen;	// 128KB

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
		
		detzone_ctrlr->mgmt_ch = spdk_bdev_get_io_channel(detzone_ctrlr->base_desc);
		// Retrieve previous zone allocations.
		// We will register namespaces after that.
		ctx = calloc(1, sizeof(struct vbdev_detzone_register_ctx));
		ctx->detzone_ctrlr = detzone_ctrlr;
		ctx->cb = cb;
		ctx->cb_arg = cb_arg;
		ctx->zone_md = calloc(detzone_ctrlr->num_zones, sizeof(struct vbdev_detzone_zone_md));
		ctx->ext_info = calloc(detzone_ctrlr->num_zones, sizeof(struct spdk_bdev_zone_ext_info));
		rc = spdk_bdev_get_zone_ext_info(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
						0, detzone_ctrlr->num_zones, ctx->ext_info,
						_vbdev_detzone_init_zone_info_cb, ctx);
		if (rc) {
			SPDK_ERRLOG("Unable to get init zone info of the detzone bdev %s.\n", detzone_ctrlr->mgmt_bdev.name);
			spdk_bdev_module_release_bdev(bdev);
			free(ctx->ext_info);
			free(ctx);
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
spdk_bdev_create_detzone_ns(const char *detzone_name, const char *ns_name,
					uint32_t zone_stripe_width, uint32_t stripe_size, uint32_t block_align,
					uint64_t num_base_zones)
{
	struct ns_association *assoc;
	int rc = 0;

	if (zone_stripe_width > DETZONE_MAX_STRIPE_WIDTH) {
		return -EINVAL;
	}
	rc = vbdev_detzone_ns_insert_association(detzone_name, ns_name,
										 zone_stripe_width, stripe_size, block_align,
										 num_base_zones);
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
spdk_bdev_delete_detzone_ns(struct spdk_bdev *bdev, spdk_bdev_unregister_cb cb_fn, void *cb_arg)
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
spdk_bdev_create_detzone_disk(const char *bdev_name, const char *vbdev_name, uint32_t num_pu,
								vbdev_detzone_register_cb cb, void *ctx)
{
	struct bdev_association *assoc;
	int rc = 0;

	rc = vbdev_detzone_insert_association(bdev_name, vbdev_name, num_pu);
	if (rc) {
		return rc;
	}

	rc = vbdev_detzone_register(bdev_name, cb, ctx);
	if (rc == -ENODEV) {
		/* This is not an error, we tracked the name above and it still
		 * may show up later.
		 */
		SPDK_NOTICELOG("vbdev creation deferred until the base bdev arrival\n");
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
spdk_bdev_delete_detzone_disk(struct spdk_bdev *bdev, spdk_bdev_unregister_cb cb_fn, void *cb_arg)
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
vbdev_detzone_examine_cb(void *arg, int rc)
{
	spdk_bdev_module_examine_done(&detzone_if);
}

static void
vbdev_detzone_examine(struct spdk_bdev *bdev)
{
	if (vbdev_detzone_register(bdev->name, vbdev_detzone_examine_cb, NULL)) {
		spdk_bdev_module_examine_done(&detzone_if);
	}
}

SPDK_LOG_REGISTER_COMPONENT(vbdev_detzone)

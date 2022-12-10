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

#define DETZONE_EWMA_SMOOTH_PARAM 4
#define DETZONE_EWMA(ewma, new) \
	((new >> DETZONE_EWMA_SMOOTH_PARAM) + (ewma) - (ewma >> DETZONE_EWMA_SMOOTH_PARAM))

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
static void vbdev_detzone_ns_alloc_zone(void *arg);
static void _vbdev_detzone_md_read_submit(struct vbdev_detzone_md_io_ctx *md_io_ctx);
static void _detzone_ns_io_read_submit(void *arg);
static int _detzone_ns_io_write_submit(struct spdk_io_channel *ch, struct vbdev_detzone_ns *detzone_ns, struct vbdev_detzone_ns_zone *zone);
static void _detzone_ns_write_prepare(void *zone);
#ifndef DETZONE_OVERDRIVE
static void vbdev_detzone_ns_shrink_zone_read(void *arg);
static void vbdev_detzone_ns_shrink_zone_copy_start(void *arg);
static void vbdev_detzone_ns_shrink_zone_alloc(void *arg);
static void vbdev_detzone_ns_shrink_zone_submit(void *arg);
static void vbdev_detzone_ns_shrink_zone(struct vbdev_detzone_ns *detzone_ns, uint64_t zone_id, void *arg);
#endif
static void _vbdev_detzone_ns_submit_request_read(void *arg);

static inline uint64_t
_vbdev_detzone_ns_get_zone_idx(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	return slba >> spdk_u64log2(detzone_ns->detzone_ns_bdev.zone_size);
}

static inline uint64_t
_vbdev_detzone_ns_get_zone_id(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	return slba & ~(detzone_ns->detzone_ns_bdev.zone_size - 1);
}

static inline uint64_t
_vbdev_detzone_ns_get_zone_id_by_idx(struct vbdev_detzone_ns *detzone_ns, uint64_t idx)
{
	return detzone_ns->internal.zone[idx].zone_id;
}

static inline enum spdk_bdev_zone_state
_vbdev_detzone_ns_get_zone_state(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	uint64_t zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, slba);

	return detzone_ns->internal.zone[zone_idx].state;
}

static inline void*
_vbdev_detzone_ns_get_zone_is_shrink(struct vbdev_detzone_ns *detzone_ns, uint64_t slba, bool sel_all)
{
	uint64_t zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, slba);

	if (sel_all) {
		uint32_t num_zones = detzone_ns->detzone_ns_bdev.blockcnt /
												 detzone_ns->detzone_ns_bdev.zone_size;
		for (uint32_t i = 0; i < num_zones; i++) {
			if (detzone_ns->internal.zone[i].shrink_ctx) {
				return detzone_ns->internal.zone[i].shrink_ctx;
			}
		}
	} else if (detzone_ns->internal.zone[zone_idx].shrink_ctx) {
		return detzone_ns->internal.zone[zone_idx].shrink_ctx;
	}
	return NULL;
}

static inline uint64_t
_vbdev_detzone_ns_get_zone_wp(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	uint64_t zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, slba);

	return detzone_ns->internal.zone[zone_idx].write_pointer;
}

static inline uint64_t
_vbdev_detzone_ns_get_zone_cap(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	uint64_t zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, slba);

	return detzone_ns->internal.zone[zone_idx].capacity;
}

static inline uint64_t
_vbdev_detzone_ns_get_sgrp_idx(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	uint64_t zone_idx = slba / detzone_ns->detzone_ns_bdev.zone_size;
	uint64_t sgrp_idx = (slba - detzone_ns->internal.zone[zone_idx].zone_id) / detzone_ns->base_avail_zcap;
	assert(sgrp_idx < DETZONE_MAX_STRIPE_WIDTH);
	return sgrp_idx;
}

static inline uint64_t
_vbdev_detzone_ns_get_phy_offset(struct vbdev_detzone_ns *detzone_ns, uint64_t slba)
{
	struct vbdev_detzone_ns_zone *zone;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	uint64_t zone_idx = slba >> spdk_u64log2(detzone_ns->detzone_ns_bdev.zone_size);
	uint64_t stride_grp_idx;
	uint64_t basezone_idx;
	uint64_t stripe_offset;

	zone = &detzone_ns->internal.zone[zone_idx];
	//if (slba >= zone->zone_id + zone->capacity) {
	if (slba - zone->zone_id 
			>= zone->num_zone_alloc * detzone_ns->base_avail_zcap) {
		if (slba < zone->zone_id + zone->capacity) {
			stride_grp_idx = (slba - zone->zone_id) / detzone_ns->base_avail_zcap;
			stripe_group = &zone->stripe_group[stride_grp_idx];
			assert(stripe_group->slba == UINT64_MAX);
		}
		return UINT64_MAX;
	}
	//SPDK_DEBUGLOG(vbdev_detzone, "slba: %lu zone_idx: %lu  basezone_idx: %lu  stripe_offset: %lu basezone_id: 0x%lx \n",
	//					 slba, zone_idx, basezone_idx, stripe_offset, zone->base_zone_id[basezone_idx]);
	//if (stripe_group->slba == UINT64_MAX) {
	//	return UINT64_MAX;
	//}
	stride_grp_idx = (slba - zone->zone_id) / detzone_ns->base_avail_zcap;
	stripe_group = &zone->stripe_group[stride_grp_idx];
	assert(stripe_group->slba != UINT64_MAX);
	basezone_idx = stripe_group->base_start_idx + (((slba - stripe_group->slba) / stripe_group->stripe_blks) % stripe_group->width);
	stripe_offset = ((slba - stripe_group->slba) / (stripe_group->width * stripe_group->stripe_blks)) * stripe_group->stripe_blks;
	return zone->base_zone[basezone_idx].zone_id
				+ detzone_ns->padding_blocks  // This is offset for the zone metadata (kind of workaround for the current F/W)
											// first block is written at the reservation time, second is at the allocation time
				+ stripe_offset + ((slba - stripe_group->slba) % stripe_group->stripe_blks);
}

static inline uint64_t
_vbdev_detzone_ns_get_phy_zone_wp(struct vbdev_detzone_ns *detzone_ns, uint64_t phy_offset)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	uint64_t zone_idx = phy_offset / detzone_ctrlr->mgmt_bdev.zone_size;

	return detzone_ctrlr->zone_info[zone_idx].write_pointer;
}

static int
_vbdev_detzone_acquire_phy_actives(void *arg)
{
	struct vbdev_detzone *detzone_ctrlr = arg;
	assert(detzone_ctrlr->thread == spdk_get_thread());

	if (detzone_ctrlr->num_zone_active == detzone_ctrlr->mgmt_bdev.max_active_zones) {
		return -EAGAIN;
	}
	detzone_ctrlr->num_zone_active += 1;
	//SPDK_DEBUGLOG(vbdev_detzone, "phy zone resource acquired (%u)\n", detzone_ctrlr->num_zone_active);
	return 0;
}

static void
_vbdev_detzone_release_phy_actives(void *arg)
{
	struct vbdev_detzone *detzone_ctrlr = arg;
	if (detzone_ctrlr->thread != spdk_get_thread()) {
		spdk_thread_send_msg(detzone_ctrlr->thread, _vbdev_detzone_release_phy_actives, detzone_ctrlr);
		return;
	}

	detzone_ctrlr->num_zone_active -= 1;
	//spdk_thread_send_msg(detzone_ctrlr->thread, vbdev_detzone_reserve_zone, detzone_ctrlr);
	//SPDK_DEBUGLOG(vbdev_detzone, "phy zone resource released (%u)\n", detzone_ctrlr->num_zone_active);
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
_verify_num_open_zone(struct vbdev_detzone_ns *detzone_ns, uint32_t num_opens)
{
	struct vbdev_detzone_ns_zone *zone = &detzone_ns->internal.zone[0];
	uint64_t total_lzones, i;
	uint32_t answer = 0;

	total_lzones = detzone_ns->detzone_ns_bdev.blockcnt / detzone_ns->detzone_ns_bdev.zone_size;
	for (i=0; i < total_lzones; i++) {
		if (!_detzone_is_open_state(zone[i].state)) {
			continue;
		}

		answer += 1;
	}
	if (num_opens != answer) {
		_dump_zone_info(detzone_ns);
	}
	assert(num_opens == answer);
}

static void
_verify_active_zone_list(struct vbdev_detzone_ns *detzone_ns)
{
	struct vbdev_detzone_ns_zone *zone = &detzone_ns->internal.zone[0];
	uint64_t total_lzones, i;

	total_lzones = detzone_ns->detzone_ns_bdev.blockcnt / detzone_ns->detzone_ns_bdev.zone_size;
	for (i=0; i < total_lzones; i++) {
		if (!_detzone_is_active_state(zone[i].state)) {
			continue;
		}
		struct vbdev_detzone_ns_zone *tmp_zone;
		TAILQ_FOREACH(tmp_zone, &detzone_ns->internal.active_zones, active_link) {
			if (tmp_zone == &zone[i]) {
				break;
			}
		}
		if (tmp_zone == NULL) {
			_dump_zone_info(detzone_ns);
			assert(tmp_zone);
		}
	}
}

static inline void
_vbdev_detzone_ns_forward_phy_zone_wp(struct vbdev_detzone_ns *detzone_ns, uint64_t phy_offset, uint64_t numblocks)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	uint64_t zone_idx = phy_offset / detzone_ctrlr->mgmt_bdev.zone_size;
	uint64_t written_blks;

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
		if (_detzone_is_active_state(detzone_ctrlr->zone_info[zone_idx].state)) {
			_vbdev_detzone_release_phy_actives(detzone_ctrlr);
			detzone_ns->num_phy_active_zones -= 1;
		}
		if (_detzone_is_open_state(detzone_ctrlr->zone_info[zone_idx].state)) {
			detzone_ns->num_phy_open_zones -= 1;
		}
		detzone_ctrlr->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_FULL;
	}
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

struct vbdev_detzone_ns_stripe_group empty_stripe_group = {
	.slba = UINT64_MAX,
	.width = 0,
	.stripe_blks = 0,
	.base_start_idx = 0,
};

static void
_detzone_zone_management_complete(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = arg;

	if (mgmt_io_ctx->submitted_thread != spdk_get_thread()) {
		spdk_thread_send_msg(mgmt_io_ctx->submitted_thread,
									 _detzone_zone_management_complete, mgmt_io_ctx);
		return;
	}

	mgmt_io_ctx->cb(mgmt_io_ctx->parent_io,
						 mgmt_io_ctx->nvme_status.sct, mgmt_io_ctx->nvme_status.sc,
						 mgmt_io_ctx->cb_arg);
	free(mgmt_io_ctx);
}

static void
_detzone_zone_management_update_phy(struct vbdev_detzone_ns *detzone_ns, uint64_t phy_zone_id, int action)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *) detzone_ns->ctrl;
	uint64_t zone_idx = phy_zone_id / detzone_ctrlr->mgmt_bdev.zone_size;
	switch (action) {
	case SPDK_BDEV_ZONE_RESET:
		if (_detzone_is_active_state(detzone_ctrlr->zone_info[zone_idx].state)) {
			_vbdev_detzone_release_phy_actives(detzone_ctrlr);
			detzone_ns->num_phy_active_zones -= 1;
		}
		if (_detzone_is_open_state(detzone_ctrlr->zone_info[zone_idx].state)) {
			detzone_ns->num_phy_open_zones -= 1;
		}
		detzone_ctrlr->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_EMPTY;
		break;
	case SPDK_BDEV_ZONE_CLOSE:
		if (_detzone_is_open_state(detzone_ctrlr->zone_info[zone_idx].state)) {
			detzone_ns->num_phy_open_zones -= 1;
		}
		detzone_ctrlr->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_CLOSED;
		break;
	case SPDK_BDEV_ZONE_FINISH:
		if (_detzone_is_active_state(detzone_ctrlr->zone_info[zone_idx].state)) {
			_vbdev_detzone_release_phy_actives(detzone_ctrlr);
			detzone_ns->num_phy_active_zones -= 1;
		}
		if (_detzone_is_open_state(detzone_ctrlr->zone_info[zone_idx].state)) {
			detzone_ns->num_phy_open_zones -= 1;
		}
		detzone_ctrlr->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_FULL;
		break;
	case SPDK_BDEV_ZONE_OPEN:
		if (!_detzone_is_active_state(detzone_ctrlr->zone_info[zone_idx].state)) {
			detzone_ns->num_phy_active_zones += 1;
		}
		if (!_detzone_is_open_state(detzone_ctrlr->zone_info[zone_idx].state)) {
			detzone_ns->num_phy_open_zones += 1;
		}
		detzone_ctrlr->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_EXP_OPEN;
		break;
	case SPDK_BDEV_ZONE_OFFLINE:
		if (_detzone_is_active_state(detzone_ctrlr->zone_info[zone_idx].state)) {
			_vbdev_detzone_release_phy_actives(detzone_ctrlr);
			detzone_ns->num_phy_active_zones -= 1;
		}
		if (_detzone_is_open_state(detzone_ctrlr->zone_info[zone_idx].state)) {
			detzone_ns->num_phy_open_zones -= 1;
		}
		detzone_ctrlr->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_OFFLINE;
		break;
	default:
		assert(0);
	}

	/* SPDK_DEBUGLOG(vbdev_detzone, "zone stats after mgmt_done(%s): logical(%u/%u) phy actives(%u) phy opens(%u)\n",
										action_str[action],
										detzone_ns->num_open_zones, detzone_ns->num_active_zones,
										detzone_ns->num_phy_active_zones, detzone_ns->num_phy_open_zones); */
}

static void
_detzone_zone_management_done(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = cb_arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_ns_zone *zone;
	uint64_t zone_idx, i;

	if (!success) {
		mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
		spdk_bdev_io_get_nvme_status(bdev_io, &mgmt_io_ctx->nvme_status.cdw0,
							 &mgmt_io_ctx->nvme_status.sct, &mgmt_io_ctx->nvme_status.sc);
	} else {
		_detzone_zone_management_update_phy(detzone_ns,
												 bdev_io->u.zone_mgmt.zone_id,
												 bdev_io->u.zone_mgmt.zone_action);
	}
	spdk_bdev_free_io(bdev_io);

	assert(mgmt_io_ctx->outstanding_mgmt_ios);
	mgmt_io_ctx->outstanding_mgmt_ios--;

	if (mgmt_io_ctx->outstanding_mgmt_ios == 0 && mgmt_io_ctx->remain_ios == 0) {
		zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
		if (mgmt_io_ctx->status == SPDK_BDEV_IO_STATUS_SUCCESS) {
			for (i = 0; i < mgmt_io_ctx->zone_mgmt.num_zones; i++) {
				//SPDK_DEBUGLOG(vbdev_detzone, "Zone transition: %u\n", zone->state);
				zone = &detzone_ns->internal.zone[zone_idx + i];
				if (zone->mgmt_in_progress) {
					zone->mgmt_in_progress = 0;
				} else {
					continue;
				}
				switch (mgmt_io_ctx->zone_mgmt.zone_action) {
				case SPDK_BDEV_ZONE_CLOSE:
					if (_detzone_is_open_state(zone->state)) {
						detzone_ns->num_open_zones--;
						// SPDK_DEBUGLOG(vbdev_detzone, "CLOSE: zone_id(0x%lx) wp(0x%lx) release open(%u)\n",
						// 									zone->zone_id,
						// 									zone->write_pointer,
						// 									detzone_ns->num_open_zones);
					}
					zone->state = SPDK_BDEV_ZONE_STATE_CLOSED;
					// Shrink zone stripe group
					//vbdev_detzone_ns_shrink_zone(detzone_ns, zone->zone_id, NULL);
					break;
				case SPDK_BDEV_ZONE_FINISH:
					if (_detzone_is_open_state(zone->state)) {
						detzone_ns->num_open_zones--;
						// SPDK_DEBUGLOG(vbdev_detzone, "FINISH: release open(%u)\n", detzone_ns->num_open_zones);
					}
					if (_detzone_is_active_state(zone->state)) {
						detzone_ns->num_active_zones--;
						TAILQ_REMOVE(&detzone_ns->internal.active_zones, &detzone_ns->internal.zone[zone_idx + i], active_link);
						// SPDK_DEBUGLOG(vbdev_detzone, "FINISH: release active(%u)\n", detzone_ns->num_active_zones);
					}
					if (zone->write_pointer != zone->num_zone_alloc * detzone_ns->base_avail_zcap) {
						detzone_ns->internal.used_spares -= zone->stripe_group[zone->num_zone_alloc - 1].width - detzone_ctrlr->base_width;
					}
					zone->state = SPDK_BDEV_ZONE_STATE_FULL;
					break;
				case SPDK_BDEV_ZONE_OPEN:
					if (!_detzone_is_open_state(zone->state)) {
						detzone_ns->num_open_zones++;
						// SPDK_DEBUGLOG(vbdev_detzone, "OPEN: acquire open(%u)\n", detzone_ns->num_open_zones);
					}
					if (!_detzone_is_active_state(zone->state)) {
						detzone_ns->num_active_zones++;
						TAILQ_INSERT_TAIL(&detzone_ns->internal.active_zones, &detzone_ns->internal.zone[zone_idx + i], active_link);
						// SPDK_DEBUGLOG(vbdev_detzone, "OPEN: acquire active(%u)\n", detzone_ns->num_active_zones);
					} else {
						TAILQ_REMOVE(&detzone_ns->internal.active_zones, &detzone_ns->internal.zone[zone_idx + i], active_link);
						TAILQ_INSERT_TAIL(&detzone_ns->internal.active_zones, &detzone_ns->internal.zone[zone_idx + i], active_link);
					}
					zone->state = SPDK_BDEV_ZONE_STATE_EXP_OPEN;
					break;
				case SPDK_BDEV_ZONE_RESET:
					switch (zone->state) {
					case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
					case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
						detzone_ns->num_open_zones--;
						// SPDK_DEBUGLOG(vbdev_detzone, "RESET: release open(%u)\n", detzone_ns->num_open_zones);
						/* fall through */
					case SPDK_BDEV_ZONE_STATE_CLOSED:
						detzone_ns->num_active_zones--;
						TAILQ_REMOVE(&detzone_ns->internal.active_zones, &detzone_ns->internal.zone[zone_idx + i], active_link);
						// SPDK_DEBUGLOG(vbdev_detzone, "RESET: release active(%u)\n", detzone_ns->num_active_zones);
						if (zone->write_pointer != zone->num_zone_alloc * detzone_ns->base_avail_zcap) {
							detzone_ns->internal.used_spares -= zone->stripe_group[zone->num_zone_alloc - 1].width - detzone_ctrlr->base_width;
						}
						/* fall through */
					case SPDK_BDEV_ZONE_STATE_FULL:
						zone->state = SPDK_BDEV_ZONE_STATE_EMPTY;
						zone->write_pointer = zone->zone_id;
						zone->last_write_pointer[0] = zone->zone_id + zone->capacity;
						zone->last_write_pointer[1] = zone->zone_id + zone->capacity;
						zone->last_write_pointer[2] = zone->zone_id + zone->capacity;
						vbdev_detzone_ns_dealloc_zone(detzone_ns, zone_idx + i);
						/* fall through */
					default:
						break;
					}
					
					break;
				case SPDK_BDEV_ZONE_OFFLINE:
					/*
					assert(zone->state == SPDK_BDEV_ZONE_STATE_READ_ONLY);
					zone->state = SPDK_BDEV_ZONE_STATE_OFFLINE;
					zone->write_pointer = zone->zone_id;
					zone->last_write_pointer[0] = zone->zone_id;
					zone->last_write_pointer[1] = zone->zone_id;
					zone->last_write_pointer[2] = zone->zone_id;
					vbdev_detzone_ns_dealloc_zone(detzone_ns, zone_idx + i);
					break;
					*/
				default:
					assert(0);
				}
				//_verify_active_zone_list(detzone_ns);
			}
		} else {
			// TODO: No recovery now. We may fix zone states in the future...
			assert(0);
			for (i = 0; i < mgmt_io_ctx->zone_mgmt.num_zones; i++) {
				zone->state = SPDK_BDEV_ZONE_STATE_OFFLINE;
				vbdev_detzone_ns_dealloc_zone(detzone_ns, zone_idx + i);
			}
		}
		_detzone_zone_management_complete(mgmt_io_ctx);
	}

	return;
}

static void
vbdev_detzone_ns_dealloc_zone(struct vbdev_detzone_ns *detzone_ns, uint64_t zone_idx)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	uint64_t dealloc_zone_idx;
	uint32_t i;

	assert(detzone_ctrlr->thread == spdk_get_thread());

	for (i = 0; i < detzone_ns->internal.zone[zone_idx].num_zone_alloc; i++) {
		detzone_ns->internal.zone[zone_idx].stripe_group[i] = empty_stripe_group;

		assert(detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id != UINT64_MAX);
		dealloc_zone_idx = detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id /
								detzone_ns->base_zsze;
		detzone_ns->internal.zone[zone_idx].base_zone[i].zone_id = UINT64_MAX;
		detzone_ctrlr->zone_info[dealloc_zone_idx].ns_id = 0;
		detzone_ctrlr->zone_info[dealloc_zone_idx].lzone_id = 0;
		detzone_ctrlr->zone_info[dealloc_zone_idx].stripe_id = 0;
		detzone_ctrlr->zone_info[dealloc_zone_idx].stripe_group = empty_stripe_group;
		detzone_ctrlr->zone_info[dealloc_zone_idx].pu_group = detzone_ctrlr->num_pu;

		// !!!! NEED CHECK AGAIN !!!!
		if (detzone_ctrlr->zone_info[dealloc_zone_idx].state == SPDK_BDEV_ZONE_STATE_FULL) {
			// noop
		} else if (detzone_ns->internal.zone[zone_idx].state == SPDK_BDEV_ZONE_STATE_EMPTY) {
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
	detzone_ns->internal.zone[zone_idx].num_zone_alloc = 0;
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
	struct vbdev_detzone_zone_info *phy_zone;
	struct vbdev_detzone_ns_shrink_zone_ctx *shrink_ctx;
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx;
	int rc;

	assert(detzone_ctrlr->thread == spdk_get_thread());

	phy_zone = TAILQ_FIRST(&detzone_ctrlr->zone_empty);
	assert(phy_zone); // TODO: it's overprovisioning case. we don't support yet.

	if (phy_zone->pu_group != detzone_ctrlr->num_pu) {
		// SPDK_DEBUGLOG(vbdev_detzone, "ongoing reservation process found (zone_id:0x%lx)\n",
		// 										phy_zone->zone_id);
		// there is an on-going reservation. ignore redundant request.
		return;
	}

#ifdef DETZONE_OVERDRIVE
	shrink_ctx = NULL;
#else
	// For the consistency of zone metadata, we must process pending shrinks first.
	shrink_ctx = TAILQ_FIRST(&detzone_ctrlr->internal.zone_shrink_queued);
	if (shrink_ctx && shrink_ctx->num_alloc_require <= detzone_ctrlr->num_zone_reserved) {
		shrink_ctx = TAILQ_FIRST(&detzone_ctrlr->internal.zone_shrink_queued);
		SPDK_DEBUGLOG(vbdev_detzone, "Call shrink: zone_id(0x%lx)\n", shrink_ctx->org_zone->zone_id);
		vbdev_detzone_ns_shrink_zone_alloc(shrink_ctx);
		return;
	}
#endif
	mgmt_io_ctx = TAILQ_FIRST(&detzone_ctrlr->internal.zone_alloc_queued);
	if (!shrink_ctx && mgmt_io_ctx && mgmt_io_ctx->num_alloc_require <= detzone_ctrlr->num_zone_reserved) {
		// SPDK_DEBUGLOG(vbdev_detzone, "Call alloc: zone_id(0x%lx)\n", mgmt_io_ctx->zone_mgmt.zone_id);
		vbdev_detzone_ns_alloc_zone(mgmt_io_ctx);
		return;
	}

	if (detzone_ctrlr->num_zone_active >= detzone_ctrlr->mgmt_bdev.max_active_zones) {
		SPDK_DEBUGLOG(vbdev_detzone, "No more available active resources to reserve: rsvd(%u) actives(%u) max(%u)\n",
											detzone_ctrlr->num_zone_reserved,
											detzone_ctrlr->num_zone_active,
											detzone_ctrlr->mgmt_bdev.max_active_zones);
		assert(detzone_ctrlr->num_zone_active < detzone_ctrlr->mgmt_bdev.max_active_zones);
		return;
	} else if (detzone_ctrlr->num_zone_reserved >= DETZONE_MAX_RESERVE_ZONES) {
		//SPDK_DEBUGLOG(vbdev_detzone, "Resevered the maximum zones: %u\n", detzone_ctrlr->num_zone_reserved);
		return;
	}

	if (0 != (rc = _vbdev_detzone_acquire_phy_actives(detzone_ctrlr))) {
		SPDK_ERRLOG("cannot acquire zone for reservation\n");
		return;
	}

	//SPDK_DEBUGLOG(vbdev_detzone, "reserving a zone (zone_id:0x%lx @ %p)\n",
	//										phy_zone->zone_id, phy_zone);
	phy_zone->pu_group = (detzone_ctrlr->zone_alloc_cnt + 1) % detzone_ctrlr->num_pu;
	rc = spdk_bdev_write_zeroes_blocks(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
										phy_zone->zone_id, DETZONE_RESERVATION_BLKS, _vbdev_detzone_reserve_zone_cb,
										detzone_ctrlr);
	assert(!rc);
	return;
}

static struct vbdev_detzone_zone_info *
_vbdev_detzone_ns_get_phyzone(struct vbdev_detzone_ns *detzone_ns)
{
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	struct vbdev_detzone_zone_info *phy_zone = NULL;

	TAILQ_FOREACH(phy_zone, &detzone_ctrlr->zone_reserved, link) {
		if (spdk_bit_array_get(detzone_ns->internal.epoch_pu_map, phy_zone->pu_group)) {
			continue;
		}
		break;
	}

	if (phy_zone == NULL) {
		phy_zone = TAILQ_FIRST(&detzone_ctrlr->zone_reserved);
	}

	assert(phy_zone);
	spdk_bit_array_set(detzone_ns->internal.epoch_pu_map, phy_zone->pu_group);
	detzone_ns->internal.epoch_num_pu++;
	TAILQ_REMOVE(&detzone_ctrlr->zone_reserved, phy_zone, link);
	detzone_ctrlr->num_zone_reserved -= 1;

	if (detzone_ns->internal.epoch_num_pu >= detzone_ctrlr->num_pu) {
		spdk_bit_array_clear_mask(detzone_ns->internal.epoch_pu_map);
		detzone_ns->internal.epoch_num_pu = 0;
	}

	detzone_ns->num_phy_active_zones += 1;
	detzone_ns->num_phy_open_zones += 1;

	return phy_zone;
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
	md_io_ctx->zone_md[zone_idx].version = zone_md->version;
	md_io_ctx->zone_md[zone_idx].ns_id = zone_md->ns_id;
	md_io_ctx->zone_md[zone_idx].lzone_id = zone_md->lzone_id;
	md_io_ctx->zone_md[zone_idx].stripe_id = zone_md->stripe_id;
	md_io_ctx->zone_md[zone_idx].stripe_group = zone_md->stripe_group;

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
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_ns_zone *zone;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	uint64_t zone_idx, sgrp_idx;

	if (!success) {
		// TODO: how to handle partial errors? set to READ_ONLY?
		mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
		spdk_bdev_io_get_nvme_status(bdev_io, &mgmt_io_ctx->nvme_status.cdw0,
							 &mgmt_io_ctx->nvme_status.sct, &mgmt_io_ctx->nvme_status.sc);
	} else {
		_vbdev_detzone_ns_forward_phy_zone_wp(detzone_ns, bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);
	}
	spdk_bdev_free_io(bdev_io);
	assert(mgmt_io_ctx->outstanding_mgmt_ios);
	mgmt_io_ctx->outstanding_mgmt_ios--;

	if (mgmt_io_ctx->outstanding_mgmt_ios == 0 && mgmt_io_ctx->remain_ios == 0) {
		spdk_mempool_put(detzone_ns->md_buf_pool, mgmt_io_ctx->zone_mgmt.buf);
		zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
		zone = &detzone_ns->internal.zone[zone_idx];
		sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
		stripe_group = &zone->stripe_group[sgrp_idx];
		if (mgmt_io_ctx->status != SPDK_BDEV_IO_STATUS_FAILED) {
			if (_detzone_is_active_state(zone->state)) {
				TAILQ_REMOVE(&detzone_ns->internal.active_zones, zone, active_link);
			}
			TAILQ_INSERT_TAIL(&detzone_ns->internal.active_zones, zone, active_link);
			if (mgmt_io_ctx->parent_io->type == SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT) {
				// EXP_OPEN
				zone->state = SPDK_BDEV_ZONE_STATE_EXP_OPEN;
				// SPDK_DEBUGLOG(vbdev_detzone, "EXP_OPEN done: zone_id(0x%lx) active/open(%u/%u) phy_active/open(%u/%u)\n",
				// 								zone->zone_id,
				// 								detzone_ns->num_active_zones, detzone_ns->num_open_zones,
				// 								detzone_ns->num_phy_active_zones, detzone_ns->num_phy_open_zones);
			} else if (zone->state == SPDK_BDEV_ZONE_STATE_EMPTY) {
				// IMP_OPEN
				zone->state = SPDK_BDEV_ZONE_STATE_IMP_OPEN;
				// SPDK_DEBUGLOG(vbdev_detzone, "IMP_OPEN done: zone_id(0x%lx) active/open(%u/%u) phy_active/open(%u/%u)\n",
				// 								zone->zone_id,
				// 								detzone_ns->num_active_zones, detzone_ns->num_open_zones,
				// 								detzone_ns->num_phy_active_zones, detzone_ns->num_phy_open_zones);
			} // No state change for the already IMP_OPEN-ed zone
			zone->tb_last_update_tsc = spdk_get_ticks();
			zone->tb_size_max = detzone_ctrlr->per_zone_mdts * stripe_group->width;
			zone->tb_size = stripe_group->stripe_blks * stripe_group->width;
			zone->tb_tokens = stripe_group->stripe_blks * stripe_group->width;
			// zone->tb_size = detzone_ctrlr->base_stripe_blks * stripe_group->width;
			// zone->tb_tokens = detzone_ctrlr->base_stripe_blks * stripe_group->width;
			zone->mgmt_in_progress = 0;
		} else if (mgmt_io_ctx->parent_io->type == SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT
					|| zone->state == SPDK_BDEV_ZONE_STATE_EMPTY) {
			// In case of internal failure, we have to decrease num of active/open zones
			detzone_ns->num_open_zones -= 1;
			detzone_ns->num_active_zones -= 1;
		}
		//_verify_active_zone_list(detzone_ns);
		_detzone_zone_management_complete(mgmt_io_ctx);
	}
}

static void
_vbdev_detzone_ns_alloc_md_write(struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx)
{
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	struct vbdev_detzone_zone_info *phy_zone;
	struct vbdev_detzone_zone_md *zone_md;
	uint64_t zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	uint64_t sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	uint64_t phy_zone_idx;
	uint32_t i;
	int rc;

	assert(!mgmt_io_ctx->zone_mgmt.select_all);
	stripe_group = &detzone_ns->internal.zone[zone_idx].stripe_group[sgrp_idx];
	mgmt_io_ctx->zone_mgmt.buf = spdk_mempool_get(detzone_ns->md_buf_pool);
	mgmt_io_ctx->remain_ios = stripe_group->width;
	for (i = 0; i < stripe_group->width; i++) {
		phy_zone_idx = detzone_ns->internal.zone[zone_idx].base_zone[stripe_group->base_start_idx + i].zone_id
							/ detzone_ns->base_zsze;
		phy_zone = &detzone_ctrlr->zone_info[phy_zone_idx];
		zone_md = (struct vbdev_detzone_zone_md *)((uint8_t*)mgmt_io_ctx->zone_mgmt.buf +
													 i * (detzone_ns->padding_blocks - DETZONE_RESERVATION_BLKS) *
													 detzone_ns->detzone_ns_bdev.blocklen);
		zone_md->version = DETZONE_NS_META_FORMAT_VER;
		zone_md->lzone_id = phy_zone->lzone_id;
		zone_md->ns_id = phy_zone->ns_id;
		zone_md->stripe_id = phy_zone->stripe_id;
		zone_md->stripe_group = phy_zone->stripe_group;
		// write metadata
		//SPDK_DEBUGLOG(vbdev_detzone, "write MD: zone_id(0x%lx) phy_zone_id(0x%lx) base_idx(%u) sgrp_idx(%lu)\n",
		//									detzone_ns->internal.zone[zone_idx].zone_id, phy_zone->zone_id, stripe_group->base_start_idx + i, sgrp_idx);
		rc = spdk_bdev_write_blocks(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
						zone_md,
						phy_zone->zone_id + DETZONE_RESERVATION_BLKS,
						detzone_ns->padding_blocks - DETZONE_RESERVATION_BLKS,
						_vbdev_detzone_ns_alloc_md_write_cb, mgmt_io_ctx);
		mgmt_io_ctx->outstanding_mgmt_ios++;
		mgmt_io_ctx->remain_ios--;
		assert(!rc);
	}
}

static void
vbdev_detzone_redist_spares(void *arg)
{
	struct vbdev_detzone *detzone_ctrlr = arg;
	struct vbdev_detzone_ns *detzone_ns;
	uint32_t ns_weight[DETZONE_MAX_NAMESPACES] = {0};
	uint32_t ns_actives[DETZONE_MAX_NAMESPACES] = {0};
	uint32_t new_baseline[DETZONE_MAX_NAMESPACES] = {0};
	uint32_t total_weight = 0;
	uint32_t harvested_spares = 0;
	uint32_t need_reclaim = 0;
	uint32_t dist_spares = 0;
	uint32_t num_active_ns = 0;
	uint32_t num_awaking_ns = 0;

#ifdef DETZONE_UBENCH
	//return;
#endif
	for (int i = 0; i < DETZONE_MAX_ALLOC_HISTORY; i++) {
		ns_weight[detzone_ctrlr->internal.alloc_history[i]] += 1;
		if (ns_actives[detzone_ctrlr->internal.alloc_history[i]] == 0) {
			ns_actives[detzone_ctrlr->internal.alloc_history[i]] = detzone_ctrlr->internal.alloc_history_actives[i];
		} else {
			ns_actives[detzone_ctrlr->internal.alloc_history[i]]
				 = DETZONE_EWMA(ns_actives[detzone_ctrlr->internal.alloc_history[i]], detzone_ctrlr->internal.alloc_history_actives[i]);
		}
	}

	for (int i = 0; i < DETZONE_MAX_NAMESPACES; i++) {
		if (ns_weight[i] != 0) {
			num_active_ns += 1;
		}
	}

	TAILQ_FOREACH(detzone_ns, &detzone_ctrlr->ns, link) {
		uint32_t total_avail_spares, unused_spares;
/* 		OLD
		total_avail_spares = detzone_ns->internal.spares + detzone_ns->internal.leased_spares
										- detzone_ns->internal.lent_spares;
		if (total_avail_spares - detzone_ns->internal.used_spares > 0) {
			harvested_spares += total_avail_spares - detzone_ns->internal.used_spares;
			detzone_ns->internal.lent_spares += total_avail_spares - detzone_ns->internal.used_spares;
		}
 */
/* 		if (detzone_ns->internal.used_spares < detzone_ns->internal.spares) {
			total_avail_spares = detzone_ns->internal.spares - detzone_ns->internal.used_spares;
			detzone_ns->internal.spares = detzone_ns->internal.used_spares;
			if (detzone_ns->internal.leased_spares > 0) {
				total_avail_spares -= spdk_min(detzone_ns->internal.leased_spares, total_avail_spares);
			}
			harvested_spares += total_avail_spares;
		}
 */	
		total_avail_spares = detzone_ns->internal.spares + detzone_ns->internal.leased_spares
									- detzone_ns->internal.lent_spares;
		unused_spares = total_avail_spares - detzone_ns->internal.used_spares;
		if (ns_weight[detzone_ns->nsid] == 0) {
			harvested_spares += unused_spares;
			detzone_ns->internal.lent_spares += unused_spares;
			if (detzone_ns->internal.leased_spares > detzone_ns->internal.lent_spares) {
				detzone_ns->internal.leased_spares -= detzone_ns->internal.lent_spares;
				detzone_ns->internal.lent_spares = 0;
			} else {
				detzone_ns->internal.lent_spares -= detzone_ns->internal.leased_spares;
				detzone_ns->internal.leased_spares = 0;
			}
		} else if (detzone_ns->internal.leased_spares > 0) {
			if (detzone_ns->internal.leased_spares > unused_spares) {
				detzone_ns->internal.leased_spares -= unused_spares;
			} else {
				unused_spares = detzone_ns->internal.leased_spares;
				detzone_ns->internal.leased_spares = 0;
			}
			if (detzone_ns->internal.reclaimed_spares > unused_spares) {
				detzone_ns->internal.reclaimed_spares -= unused_spares;
			} else {
				detzone_ns->internal.reclaimed_spares = 0;
			}
			harvested_spares += unused_spares;
		}
	}

	detzone_ctrlr->avail_spares += harvested_spares;
	//fprintf(stderr, "global overdrive: harvested(%u) total_avail(%u)\n", harvested_spares, detzone_ctrlr->avail_spares);

	TAILQ_FOREACH(detzone_ns, &detzone_ctrlr->ns, link) {
		if (ns_weight[detzone_ns->nsid] == 0) {
			continue;
		}
		if (detzone_ns->internal.lent_spares > detzone_ctrlr->avail_spares) {
			detzone_ns->internal.lent_spares -= detzone_ctrlr->avail_spares;
			detzone_ctrlr->avail_spares = 0;
			need_reclaim += detzone_ns->internal.lent_spares;
			num_awaking_ns += 1;
			break;
		} else if (detzone_ns->internal.lent_spares > 0) {
			detzone_ctrlr->avail_spares -= detzone_ns->internal.lent_spares;
			detzone_ns->internal.lent_spares = 0;
		}
	}

	if (detzone_ctrlr->avail_spares > 0) {
		TAILQ_FOREACH(detzone_ns, &detzone_ctrlr->ns, link) {
			// fprintf(stderr, "global overdrive: nsid(%u) weight(%u) org_spares(%u+%u-%u) ", detzone_ns->nsid, ns_weight[detzone_ns->nsid],
			// 				detzone_ns->internal.spares, detzone_ns->internal.leased_spares, detzone_ns->internal.lent_spares);
			if (ns_weight[detzone_ns->nsid] != 0) {
				detzone_ns->internal.leased_spares += detzone_ctrlr->avail_spares / num_active_ns;
				if (detzone_ns->internal.lent_spares > detzone_ns->internal.leased_spares) {
					detzone_ns->internal.lent_spares -= detzone_ns->internal.leased_spares;
					detzone_ns->internal.leased_spares = 0;
				} else {
					detzone_ns->internal.leased_spares -= detzone_ns->internal.lent_spares;
					detzone_ns->internal.lent_spares = 0;
				}
				detzone_ctrlr->avail_spares -= detzone_ctrlr->avail_spares / num_active_ns;
			}
			// fprintf(stderr, "new_spares(%u+%u-%u)\n",
			// 				detzone_ns->internal.spares, detzone_ns->internal.leased_spares, detzone_ns->internal.lent_spares);
		}
	} else if (need_reclaim) {
		TAILQ_FOREACH(detzone_ns, &detzone_ctrlr->ns, link) {
			if (ns_weight[detzone_ns->nsid] != 0 && detzone_ns->internal.leased_spares > 0) {
				if (detzone_ns->internal.leased_spares > need_reclaim) {
					detzone_ns->internal.reclaimed_spares = need_reclaim;
					need_reclaim = 0;
				} else {
					need_reclaim -= detzone_ns->internal.leased_spares;
					detzone_ns->internal.reclaimed_spares = detzone_ns->internal.leased_spares;
				}
			}
		}
	}
/* 
	for (int i = 0; i < DETZONE_MAX_NAMESPACES; i++) {
		if (ns_weight[i] != 0) {
			ns_weight[i] = (ns_weight[i] << 10) / ns_actives[i];
		}
		total_weight += ns_weight[i];
	}

	// setup new baseline
	for (int i = 0; i < DETZONE_MAX_NAMESPACES; i++) {
		new_baseline[i] = (detzone_ctrlr->base_spares * detzone_ctrlr->max_ns)
								* ns_weight[i] / total_weight;
	}

	TAILQ_FOREACH(detzone_ns, &detzone_ctrlr->ns, link) {
		fprintf(stderr, "global overdrive: nsid(%u) weight(%u) org_spares(%u-%u+%u) ", detzone_ns->nsid, ns_weight[detzone_ns->nsid],
						detzone_ns->internal.spares, detzone_ns->internal.leased_spares, detzone_ns->internal.lent_spares);
		detzone_ns->internal.leased_spares = 0;
		detzone_ns->internal.lent_spares = 0;
		if (detzone_ns->internal.spares > new_baseline[detzone_ns->nsid]) {
			detzone_ns->internal.leased_spares = detzone_ns->internal.spares - new_baseline[detzone_ns->nsid];
		} else {
			detzone_ns->internal.lent_spares = new_baseline[detzone_ns->nsid] - detzone_ns->internal.spares;
		}
		fprintf(stderr, "new_spares(%u-%u+%u)\n",
						detzone_ns->internal.spares, detzone_ns->internal.leased_spares, detzone_ns->internal.lent_spares);
	}
 */

/* 		OLD
	TAILQ_FOREACH(detzone_ns, &detzone_ctrlr->ns, link) {
		fprintf(stderr, "global overdrive: nsid(%u) weight(%u) org_spares(%u+%u-%u) ", detzone_ns->nsid, ns_weight[detzone_ns->nsid],
						detzone_ns->internal.spares, detzone_ns->internal.leased_spares, detzone_ns->internal.lent_spares);
		dist_spares += detzone_ctrlr->avail_spares
						 * ns_weight[detzone_ns->nsid] / DETZONE_MAX_ALLOC_HISTORY;
		detzone_ns->internal.leased_spares += detzone_ctrlr->avail_spares
						 * ns_weight[detzone_ns->nsid] / DETZONE_MAX_ALLOC_HISTORY;
		if (detzone_ns->internal.lent_spares > detzone_ns->internal.leased_spares) {
			detzone_ns->internal.lent_spares -= detzone_ns->internal.leased_spares;
			detzone_ns->internal.leased_spares = 0;
		} else {
			detzone_ns->internal.leased_spares -= detzone_ns->internal.lent_spares;
			detzone_ns->internal.lent_spares = 0;
		}
		fprintf(stderr, "new_spares(%u+%u-%u)\n",
						detzone_ns->internal.spares, detzone_ns->internal.leased_spares, detzone_ns->internal.lent_spares);
	}
 */
	// assert(dist_spares >= detzone_ctrlr->avail_spares);
	// detzone_ctrlr->avail_spares -= dist_spares;
}

static void
vbdev_detzone_ns_alloc_zone(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = (struct vbdev_detzone_ns_mgmt_io_ctx *)arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	struct vbdev_detzone_ns_zone *zone, *tmp_zone, *victim_zone;
	struct vbdev_detzone_zone_info *phy_zone;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	//bool do_notification = false;
	uint64_t zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	uint64_t sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	uint32_t basezone_offset = detzone_ns->internal.zone[zone_idx].num_zone_alloc;
	uint32_t num_zone_alloc = 0;
	uint32_t width;
	uint32_t total_spares, avail_spares;

	assert(detzone_ctrlr->thread == spdk_get_thread());
	assert(mgmt_io_ctx->zone_mgmt.num_zones == 1);

	if (mgmt_io_ctx == TAILQ_FIRST(&detzone_ctrlr->internal.zone_alloc_queued)) {
		TAILQ_REMOVE(&detzone_ctrlr->internal.zone_alloc_queued, mgmt_io_ctx, link);
	} else if (!TAILQ_EMPTY(&detzone_ctrlr->internal.zone_alloc_queued)) {
		// queue the allocation
		TAILQ_INSERT_TAIL(&detzone_ctrlr->internal.zone_alloc_queued, mgmt_io_ctx, link);
		SPDK_DEBUGLOG(vbdev_detzone, "queued allocation: zone_id(0x%lx)\n", mgmt_io_ctx->zone_mgmt.zone_id);
		//spdk_thread_send_msg(detzone_ctrlr->thread, vbdev_detzone_ns_alloc_zone, arg);
		return;
	}

	//_verify_active_zone_list(detzone_ns);

	// this should not fail...
	mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_GENERIC;
	mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_SUCCESS;
 
	// get the next stripe group
	zone = &detzone_ns->internal.zone[zone_idx];
	stripe_group = &zone->stripe_group[sgrp_idx];
	assert(stripe_group->slba == UINT64_MAX);
	assert(zone->write_pointer == zone->zone_id + zone->num_zone_alloc * detzone_ns->base_avail_zcap);
	// determine the stripe width in the given resource utilization

#ifdef DETZONE_OVERDRIVE
#ifdef DETZONE_UBENCH
#ifndef DETZONE_UBENCH_OVERDRIVE
	stripe_group->width = spdk_align32pow2((detzone_ns->internal.essentials + detzone_ns->internal.spares) / 16);
	//if (detzone_ns->nsid == 4) {
	if (0) {
		stripe_group->stripe_blks =	detzone_ctrlr->base_stripe_blks;
	} else {
		stripe_group->stripe_blks = detzone_ctrlr->base_stripe_blks >> (spdk_u32log2(stripe_group->width) - spdk_u32log2(detzone_ctrlr->base_width));
	}
#endif
#else

	total_spares = (detzone_ns->internal.spares - detzone_ns->internal.lent_spares)
						+ detzone_ns->internal.leased_spares - detzone_ns->internal.reclaimed_spares;
	if (total_spares <= detzone_ns->internal.used_spares) {
		width = detzone_ctrlr->base_width;
	} else {
		avail_spares = total_spares - detzone_ns->internal.used_spares;
		width = 1u << spdk_u32log2(detzone_ctrlr->base_width + total_spares
										/ ( ((detzone_ns->internal.avg_open_zones_milli + DETZONE_SHIFT_MILLI_BIN >> 1)
											  >> DETZONE_SHIFT_MILLI_BIN)));
		if (spdk_u32log2(avail_spares + detzone_ctrlr->base_width) < spdk_u32log2(width)) {
			width = 1u << spdk_u32log2(avail_spares + detzone_ctrlr->base_width);
		}
		if (spdk_u32log2(width) - spdk_u32log2(detzone_ctrlr->base_width)
				 > spdk_u32log2(detzone_ctrlr->base_stripe_blks) - 1) {
			width = detzone_ctrlr->base_width << (spdk_u32log2(detzone_ctrlr->base_stripe_blks) - 1);
		}
	}

/* 	total_spares = detzone_ns->internal.spares - detzone_ns->internal.leased_spares
							+ detzone_ns->internal.lent_spares;
	if (total_spares <= detzone_ns->internal.used_spares) {
		width = detzone_ctrlr->base_width;
	} else {
		avail_spares = total_spares - detzone_ns->internal.lent_spares;
		width = 1u << spdk_u32log2(detzone_ctrlr->base_width + total_spares
										/ ( ((detzone_ns->internal.avg_open_zones_milli + DETZONE_SHIFT_MILLI_BIN >> 1)
											  >> DETZONE_SHIFT_MILLI_BIN)));
		if (width > (avail_spares + detzone_ctrlr->base_width)
				 && detzone_ns->internal.lent_spares > width - (avail_spares + detzone_ctrlr->base_width)) {
			uint32_t reclaims;
			reclaims = width - avail_spares - detzone_ctrlr->base_width;
			if (detzone_ctrlr->avail_spares > reclaims) {
				detzone_ctrlr->avail_spares -= reclaims;
				detzone_ns->internal.lent_spares -= reclaims;
				detzone_ns->internal.spares += reclaims;
				avail_spares = width - detzone_ctrlr->base_width;
			}
		}
		if (spdk_u32log2(avail_spares + detzone_ctrlr->base_width) < spdk_u32log2(width)) {
			width = 1u << spdk_u32log2(avail_spares + detzone_ctrlr->base_width);
		}
		if (spdk_u32log2(width) - spdk_u32log2(detzone_ctrlr->base_width)
				 > spdk_u32log2(detzone_ctrlr->base_stripe_blks) - 1) {
			width = detzone_ctrlr->base_width << (spdk_u32log2(detzone_ctrlr->base_stripe_blks) - 1);
		}
	}
 */

	if (width + zone->num_zone_alloc > DETZONE_MAX_STRIPE_WIDTH) {
		width = 1u << spdk_u32log2(DETZONE_MAX_STRIPE_WIDTH - zone->num_zone_alloc);
	}
	stripe_group->width = width;
	stripe_group->stripe_blks = detzone_ctrlr->base_stripe_blks >> (spdk_u32log2(width) - spdk_u32log2(detzone_ctrlr->base_width));

	// SPDK_DEBUGLOG(vbdev_detzone, "stripe overdrive conf: base_width(%u) base_stripe_blks(%u) conf_width(%u) conf_stripe_blks(%u)\n",
	// 												detzone_ctrlr->base_width, detzone_ctrlr->base_stripe_blks,stripe_group->width, stripe_group->stripe_blks);
#endif
#else
	if (0) {
		uint32_t phy_zone_quota = (detzone_ctrlr->mgmt_bdev.max_active_zones - DETZONE_MAX_RESERVE_ZONES) / detzone_ctrlr->num_ns;
		uint32_t min_width = phy_zone_quota / detzone_ns->detzone_ns_bdev.max_active_zones;

		if (min_width > phy_zone_quota - detzone_ns->num_phy_active_zones) {
			SPDK_DEBUGLOG(vbdev_detzone, "Not enough physical zone quota, need to shrink zones: %u < %u - %u\n",
														 min_width, phy_zone_quota, detzone_ns->num_phy_active_zones);
			mgmt_io_ctx->num_alloc_require = min_width;
			TAILQ_INSERT_HEAD(&detzone_ctrlr->internal.zone_alloc_queued, mgmt_io_ctx, link);
			SPDK_DEBUGLOG(vbdev_detzone, "queued allocation: zone_id(0x%lx)\n", mgmt_io_ctx->zone_mgmt.zone_id);
			// find a victim zone to shrink
			victim_zone = NULL;
			TAILQ_FOREACH(tmp_zone, &detzone_ns->internal.active_zones, active_link) {
				sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, tmp_zone->write_pointer);
				stripe_group = &tmp_zone->stripe_group[sgrp_idx];
				if (zone == tmp_zone
						|| _vbdev_detzone_ns_get_zone_is_shrink(detzone_ns, tmp_zone->zone_id, false)
						|| stripe_group->slba == UINT64_MAX
						|| stripe_group->width <= min_width
						|| tmp_zone->mgmt_in_progress) {
					//SPDK_DEBUGLOG(vbdev_detzone, "Skip this zone: zone_id(0x%lx) this(0x%lx) is_shrink(%u) sgrp_slba(0x%lx) width(%u) mgmt_progress(%u)\n",
					//									zone->zone_id, tmp_zone->zone_id, tmp_zone->shrink_ctx ? 1 : 0, stripe_group->slba, stripe_group->width, tmp_zone->mgmt_in_progress);
					continue;
				} else if (victim_zone == NULL && stripe_group->width > min_width) {
					// the least recently used zone will be a victim,
					// if other zones don't meet the optimal conditions
					SPDK_DEBUGLOG(vbdev_detzone, "LRU victim: zone_id(0x%lx) this(0x%lx)\n",
							zone->zone_id, tmp_zone->zone_id);
					victim_zone = tmp_zone;
				}
				if (stripe_group->width >= (min_width * 2)
					  || (tmp_zone->write_pointer - stripe_group->slba)
					  				 < (min_width * detzone_ns->base_avail_zcap)) {
					victim_zone = tmp_zone;
					SPDK_DEBUGLOG(vbdev_detzone, "Best victim: zone_id(0x%lx) this(0x%lx) width(%u)\n",
							zone->zone_id, tmp_zone->zone_id, stripe_group->width);
					break;
				}
			}
			if (victim_zone == NULL) {
				//_verify_active_zone_list(detzone_ns);
				//_dump_zone_info(detzone_ns);
				assert(victim_zone);
			}
			vbdev_detzone_ns_shrink_zone(detzone_ns, victim_zone->zone_id, NULL);
			return;
		} else {
			stripe_group->width = spdk_min(phy_zone_quota - detzone_ns->num_phy_active_zones,
											 phy_zone_quota / detzone_ns->num_open_zones);
		}

		if (stripe_group->width + zone->num_zone_alloc >
								zone->capacity / detzone_ns->base_avail_zcap) {
			stripe_group->width = zone->capacity / detzone_ns->base_avail_zcap
										- zone->num_zone_alloc;
		}
		SPDK_DEBUGLOG(vbdev_detzone, "status(%u/%u/%u) zone_id(0x%lx) sgrp(%lu) ns_phy_opens/actives(%u/%u) ctrlr_actives(%u) num_rsvd(%u) max_avail(%u) select width: %u\n",
										detzone_ns->num_open_zones,
										detzone_ns->num_active_zones,
										detzone_ns->detzone_ns_bdev.max_active_zones,
										zone->zone_id, sgrp_idx,
										detzone_ns->num_phy_open_zones,
										detzone_ns->num_phy_active_zones,
										detzone_ctrlr->num_zone_active - detzone_ctrlr->num_zone_reserved,
										detzone_ctrlr->num_zone_reserved,
										phy_zone_quota - detzone_ns->num_phy_active_zones, stripe_group->width);
	} else {
		stripe_group->width = 8;
	}
#endif

	assert(stripe_group->width > 0);

	if (spdk_unlikely(detzone_ctrlr->num_zone_reserved < stripe_group->width)) {
		// queue the allocation
		mgmt_io_ctx->num_alloc_require = stripe_group->width;
		TAILQ_INSERT_HEAD(&detzone_ctrlr->internal.zone_alloc_queued, mgmt_io_ctx, link);
		SPDK_DEBUGLOG(vbdev_detzone, "queued allocation (not enough reserved zones): %u < %u zone_id(0x%lx)\n",
												detzone_ctrlr->num_zone_reserved,
												stripe_group->width,
												zone->zone_id);
		vbdev_detzone_reserve_zone(detzone_ctrlr);
		//spdk_thread_send_msg(detzone_ctrlr->thread, vbdev_detzone_ns_alloc_zone, arg);
		return;
	}

	detzone_ns->internal.used_spares += stripe_group->width - detzone_ctrlr->base_width;
	detzone_ns->internal.avg_open_zones_milli = DETZONE_EWMA(detzone_ns->internal.avg_open_zones_milli,
													 detzone_ns->num_active_zones << DETZONE_SHIFT_MILLI_BIN);
	if (detzone_ns->internal.avg_open_zones_milli >> DETZONE_SHIFT_MILLI_BIN == 0) {
		detzone_ns->internal.avg_open_zones_milli = 1 << DETZONE_SHIFT_MILLI_BIN;
	}
	// fprintf(stderr, "alloc stats: ns_id(%u) slba(0x%x) actives(%u) opens(%u) phy_act(%u) phy_op(%u) essentials(%u) spares(%u+%u-%u-%u) used(%u) conf_width(%u) conf_stripe(%u) avg_opens(%.1f)\n",
	// 									detzone_ns->nsid,
	// 									zone->zone_id,
	// 									detzone_ns->num_active_zones,
	// 									detzone_ns->num_open_zones,
	// 									detzone_ns->num_phy_active_zones,
	// 									detzone_ns->num_phy_open_zones,
	// 									detzone_ns->internal.essentials,
	// 									detzone_ns->internal.spares,
	// 									detzone_ns->internal.leased_spares,
	// 									detzone_ns->internal.lent_spares,
	// 									detzone_ns->internal.reclaimed_spares,
	// 									detzone_ns->internal.used_spares,
	// 									stripe_group->width,
	// 									stripe_group->stripe_blks,
	// 									detzone_ns->internal.avg_open_zones_milli / ((1.0) * (1 << DETZONE_SHIFT_MILLI_BIN)));

	stripe_group->slba = zone->write_pointer;
	stripe_group->base_start_idx = basezone_offset;

	while((phy_zone = _vbdev_detzone_ns_get_phyzone(detzone_ns))) {
		zone->base_zone[basezone_offset + num_zone_alloc].zone_id = phy_zone->zone_id;
		phy_zone->ns_id = detzone_ns->nsid;
		phy_zone->lzone_id = zone->zone_id;
		phy_zone->stripe_id = basezone_offset + num_zone_alloc;
		phy_zone->stripe_group = *stripe_group;

		num_zone_alloc++;
		if (num_zone_alloc == stripe_group->width) {
			break;
		}
	}

	// update the allocation history
	for (uint32_t i = 0; i < stripe_group->width; i++) {
		detzone_ctrlr->internal.alloc_history[detzone_ctrlr->internal.alloc_history_tail] = detzone_ns->nsid;
		detzone_ctrlr->internal.alloc_history_actives[detzone_ctrlr->internal.alloc_history_tail] = detzone_ns->num_phy_active_zones;
		detzone_ctrlr->internal.alloc_history_tail = (detzone_ctrlr->internal.alloc_history_tail + 1) & (DETZONE_MAX_ALLOC_HISTORY - 1);
	}
	detzone_ctrlr->internal.alloc_history_curr_cnt += stripe_group->width;
	detzone_ctrlr->internal.alloc_history_shrink_cnt += stripe_group->width;
	if (detzone_ctrlr->internal.alloc_history_curr_cnt >= DETZONE_MAX_ALLOC_HISTORY) {
		// trigger async redistribution
		// this also may tigger global shrinking
		detzone_ctrlr->internal.alloc_history_curr_cnt = 0;
		spdk_thread_send_msg(detzone_ctrlr->thread, vbdev_detzone_redist_spares, detzone_ctrlr);
	}
	
	//SPDK_DEBUGLOG(vbdev_detzone, "New stripe group: idx(%u) slba(0x%lx) width(%u) stripe_blks(%u)\n",
	//								sgrp_idx, stripe_group->slba,
	//								stripe_group->width,
	//								stripe_group->stripe_blks);
	for (uint32_t i = 1; i < num_zone_alloc; i++) {
		// copy the stripe group info to following indexes which share the same group
		zone->stripe_group[sgrp_idx + i] = *stripe_group;
		//SPDK_DEBUGLOG(vbdev_detzone, "New stripe group: idx(%u) slba(0x%lx) width(%u) stripe_blks(%u)\n",
		//								sgrp_idx + i, zone->stripe_group[sgrp_idx + i].slba,
		//								zone->stripe_group[sgrp_idx + i].width,
		//								zone->stripe_group[sgrp_idx + i].stripe_blks);
	}
	zone->num_zone_alloc += stripe_group->width;
	_vbdev_detzone_ns_alloc_md_write(mgmt_io_ctx);
	// We schedule following reservation instead of run immediately to complete this allocation first.
	spdk_thread_send_msg(detzone_ctrlr->thread, vbdev_detzone_reserve_zone, detzone_ctrlr);
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
	struct vbdev_detzone_ns_zone *zone;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	uint64_t zone_idx, phy_zone_idx, sgrp_idx;
	uint32_t i;

	assert(detzone_ctrlr->thread == spdk_get_thread());
	assert(mgmt_io_ctx->parent_io->type != SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT);

	zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	zone = &detzone_ns->internal.zone[zone_idx];
	stripe_group = &zone->stripe_group[sgrp_idx];

	if (zone->state == SPDK_BDEV_ZONE_STATE_CLOSED && stripe_group->slba != UINT64_MAX) {
		zone->state = SPDK_BDEV_ZONE_STATE_IMP_OPEN;
		for (i = 0; i < stripe_group->width; i++) {
			phy_zone_idx = zone->base_zone[stripe_group->base_start_idx + i].zone_id /
								detzone_ns->base_zsze;
			if (!_detzone_is_open_state(detzone_ctrlr->zone_info[phy_zone_idx].state)) {
				detzone_ns->num_phy_open_zones += 1;
			}
			detzone_ctrlr->zone_info[phy_zone_idx].state = SPDK_BDEV_ZONE_STATE_IMP_OPEN;
		}
		TAILQ_REMOVE(&detzone_ns->internal.active_zones, zone, active_link);
		TAILQ_INSERT_TAIL(&detzone_ns->internal.active_zones, zone, active_link);
		zone->tb_last_update_tsc = spdk_get_ticks();
		zone->tb_size_max = detzone_ctrlr->per_zone_mdts * stripe_group->width;
		zone->tb_size = stripe_group->stripe_blks * stripe_group->width;
		zone->tb_tokens = stripe_group->stripe_blks * stripe_group->width;
		// zone->tb_size = detzone_ctrlr->base_stripe_blks * stripe_group->width;
		// zone->tb_tokens = detzone_ctrlr->base_stripe_blks * stripe_group->width;
		detzone_ns->num_open_zones++;
		// SPDK_DEBUGLOG(vbdev_detzone, "IMP_OPEN: acquire open(%u)\n", detzone_ns->num_open_zones);
		//_verify_active_zone_list(detzone_ns);
		goto complete;
	} else if (zone->state == SPDK_BDEV_ZONE_STATE_EMPTY) {
		if (detzone_ns->num_open_zones >= detzone_ns->detzone_ns_bdev.max_open_zones) {
			SPDK_ERRLOG("Too many open zones (%u)\n", detzone_ns->num_open_zones);
			mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_TOO_MANY_OPEN;
			goto complete;
		} else if (zone->state == SPDK_BDEV_ZONE_STATE_EMPTY
					&& detzone_ns->num_active_zones >= detzone_ns->detzone_ns_bdev.max_active_zones) {
			SPDK_ERRLOG("Too many active zones (%u)\n", detzone_ns->num_active_zones);
			mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_TOO_MANY_ACTIVE;
			goto complete;
		}
		// We increase the number of open/active zone here to make multiple
		// concurrent allocations to see up-to-date numbers for the width decision.
		// In case of internal failure after this point,
		// we will decrease it at _vbdev_detzone_ns_alloc_md_write_cb()
		// IMP_OPEN
		detzone_ns->num_open_zones++;
		detzone_ns->num_active_zones++;
		// SPDK_DEBUGLOG(vbdev_detzone, "IMP_OPEN: acquire open(%u) active(%u)\n",
		// 							detzone_ns->num_open_zones, detzone_ns->num_active_zones);

	}
	assert(stripe_group->slba == UINT64_MAX
			|| zone->state == SPDK_BDEV_ZONE_STATE_EMPTY);
	zone->mgmt_in_progress = 1;
	vbdev_detzone_ns_alloc_zone(mgmt_io_ctx);
	return;

complete:
	_detzone_zone_management_complete(mgmt_io_ctx);
}

static void
vbdev_detzone_ns_open_zone(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_ns_zone *zone;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	uint64_t zone_idx, phy_zone_idx, sgrp_idx;
	uint32_t i;
	int rc;

	zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	if (!mgmt_io_ctx->zone_mgmt.select_all) {
		zone = &detzone_ns->internal.zone[zone_idx];
		switch (zone->state) {
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
			goto complete;
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
			zone->state = SPDK_BDEV_ZONE_STATE_EXP_OPEN;
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
			}
			// We increase the number of open/active zone here to make multiple
			// concurrent allocations to see up-to-date numbers for the width decision.
			// In case of internal failure after this point,
			// we will decrease it at _vbdev_detzone_ns_alloc_md_write_cb()
			// EXP_OPEN
			zone->mgmt_in_progress = 1;
			detzone_ns->num_open_zones++;
			detzone_ns->num_active_zones++;
			SPDK_DEBUGLOG(vbdev_detzone, "OPEN: acquire open(%u) active(%u)\n",
									detzone_ns->num_open_zones, detzone_ns->num_active_zones);
			vbdev_detzone_ns_alloc_zone(mgmt_io_ctx);
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
		zone = &detzone_ns->internal.zone[zone_idx];
		switch (zone->state) {
		case SPDK_BDEV_ZONE_STATE_CLOSED:
			sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, zone->write_pointer);
			stripe_group = &zone->stripe_group[sgrp_idx];	
			for (i = 0, rc = 0; i < stripe_group->width && rc == 0; i++) {
				phy_zone_idx = zone->base_zone[stripe_group->base_start_idx + i].zone_id /
									detzone_ns->base_zsze;
				switch (detzone_ctrlr->zone_info[phy_zone_idx].state) {
				case SPDK_BDEV_ZONE_STATE_CLOSED:
					if (!(rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
									zone->base_zone[stripe_group->base_start_idx + i].zone_id,
									mgmt_io_ctx->zone_mgmt.zone_action, _detzone_zone_management_done, mgmt_io_ctx))) {
						mgmt_io_ctx->outstanding_mgmt_ios++;
					}
					break;
				default:
					break;
				}
				if (rc != 0) {
					break;
				}
			}
			if (rc != 0) {
				mgmt_io_ctx->remain_ios = 0;
				if (mgmt_io_ctx->outstanding_mgmt_ios != 0) {
					// error will be handled by completing IOs
					mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
					return;
				}
				mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_GENERIC;
				mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_INTERNAL_DEVICE_ERROR;
				goto complete;
			}
			zone->mgmt_in_progress = 1;
			zone->tb_last_update_tsc = spdk_get_ticks();
			zone->tb_size_max = detzone_ctrlr->per_zone_mdts * stripe_group->width;
			zone->tb_size = stripe_group->stripe_blks * stripe_group->width;
			zone->tb_tokens = stripe_group->stripe_blks * stripe_group->width;
			// zone->tb_size = detzone_ctrlr->base_stripe_blks * stripe_group->width;
			// zone->tb_tokens = detzone_ctrlr->base_stripe_blks * stripe_group->width;
			/* fall through */
		default:
			// skip zones in other states
			mgmt_io_ctx->remain_ios -= 1;
			break;
		}
		zone_idx++;
	}

	if (mgmt_io_ctx->outstanding_mgmt_ios == 0) {
		mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
		goto complete;
	}
	return;

complete:
	_detzone_zone_management_complete(mgmt_io_ctx);
	return;
}

static void
vbdev_detzone_ns_reset_zone(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_ns_zone *zone;
	uint64_t zone_idx, basezone_idx;
	uint32_t i;
	int rc;

	zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	// SPDK_DEBUGLOG(vbdev_detzone, "reset zone_id(0x%lx) curr_state(%u)\n",
	// 								 		mgmt_io_ctx->zone_mgmt.zone_id,
	// 										detzone_ns->internal.zone[zone_idx].state);
	if (!mgmt_io_ctx->zone_mgmt.select_all) {
		zone = &detzone_ns->internal.zone[zone_idx];
		switch (zone->state) {
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
		zone = &detzone_ns->internal.zone[zone_idx];
		switch (zone->state) {
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
		case SPDK_BDEV_ZONE_STATE_CLOSED:
		case SPDK_BDEV_ZONE_STATE_FULL:
			for (i = 0, rc = 0; i < zone->num_zone_alloc && rc == 0; i++) {
				basezone_idx = zone->base_zone[i].zone_id /
								detzone_ns->base_zsze;
				switch (detzone_ctrlr->zone_info[basezone_idx].state) {
				case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
				case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
				case SPDK_BDEV_ZONE_STATE_CLOSED:
				case SPDK_BDEV_ZONE_STATE_FULL:
					if (!(rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
									zone->base_zone[i].zone_id,
									mgmt_io_ctx->zone_mgmt.zone_action, _detzone_zone_management_done, mgmt_io_ctx))) {
						mgmt_io_ctx->outstanding_mgmt_ios++;
					}
					break;
				default:
					break;
				}
				if (rc != 0) {
					break;
				}
			}
			if (rc != 0) {
				mgmt_io_ctx->remain_ios = 0;
				if (mgmt_io_ctx->outstanding_mgmt_ios != 0) {
					// error will be handled by completing IOs
					mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
					return;
				}
				mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_GENERIC;
				mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_INTERNAL_DEVICE_ERROR;
				goto complete;
			}
			zone->mgmt_in_progress = 1;
			/* fall through */
		default:
			// skip zones in other states
			mgmt_io_ctx->remain_ios -= 1;
			break;
		}
		zone_idx++;
	}

	if (mgmt_io_ctx->outstanding_mgmt_ios == 0) {
		mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
		goto complete;
	}
	return;

complete:
	_detzone_zone_management_complete(mgmt_io_ctx);
	return;
}

static void
vbdev_detzone_ns_close_zone(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_ns_zone *zone;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	uint64_t zone_idx, phy_zone_idx, sgrp_idx;
	uint32_t i;
	int rc;

	zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	if (!mgmt_io_ctx->zone_mgmt.select_all) {
		zone = &detzone_ns->internal.zone[zone_idx];
		switch (zone->state) {
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
		zone = &detzone_ns->internal.zone[zone_idx];
		switch (zone->state) {
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
			sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, zone->write_pointer);
			stripe_group = &zone->stripe_group[sgrp_idx];
			for (i = 0, rc = 0; i < stripe_group->width && rc == 0; i++) {
				phy_zone_idx = zone->base_zone[stripe_group->base_start_idx + i].zone_id /
									detzone_ns->base_zsze;
				switch (detzone_ctrlr->zone_info[phy_zone_idx].state) {
				case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
				case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
					if (!(rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
									zone->base_zone[stripe_group->base_start_idx + i].zone_id,
									mgmt_io_ctx->zone_mgmt.zone_action, _detzone_zone_management_done, mgmt_io_ctx))) {
						mgmt_io_ctx->outstanding_mgmt_ios++;
					}
				default:
					break;
				}
				if (rc != 0) {
					break;
				}
			}
			if (rc != 0) {
				mgmt_io_ctx->remain_ios = 0;
				if (mgmt_io_ctx->outstanding_mgmt_ios != 0) {
					// error will be handled by completing IOs
					mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
					return;
				}
				mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_GENERIC;
				mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_INTERNAL_DEVICE_ERROR;
				goto complete;
			}
			zone->mgmt_in_progress = 1;
			/* fall through */
		default:
			// skip zones in other states
			mgmt_io_ctx->remain_ios -= 1;
			break;
		}
		zone_idx++;
	}

	if (mgmt_io_ctx->outstanding_mgmt_ios == 0) {
		mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
		goto complete;
	}
	return;

complete:
	_detzone_zone_management_complete(mgmt_io_ctx);
	return;
}

static void
vbdev_detzone_ns_finish_zone(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_ns_zone *zone;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	uint64_t zone_idx, phy_zone_idx, sgrp_idx;
	uint32_t i;
	int rc;

	zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
	if (!mgmt_io_ctx->zone_mgmt.select_all) {
		zone = &detzone_ns->internal.zone[zone_idx];
		switch (zone->state) {
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
		case SPDK_BDEV_ZONE_STATE_CLOSED:
			break;
		case SPDK_BDEV_ZONE_STATE_EMPTY:
			zone->state = SPDK_BDEV_ZONE_STATE_FULL;
			/* fall through */
		case SPDK_BDEV_ZONE_STATE_FULL:
			goto complete;
		default:
			mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_INVALID_STATE;
			goto complete;
		}
	}

	while (mgmt_io_ctx->remain_ios > 0) {
		zone = &detzone_ns->internal.zone[zone_idx];
		switch (zone->state) {
		case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
		case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
		case SPDK_BDEV_ZONE_STATE_CLOSED:
			sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, zone->write_pointer);
			stripe_group = &zone->stripe_group[sgrp_idx];	
			for (i = 0, rc = 0; i < stripe_group->width && rc == 0; i++) {
				phy_zone_idx = zone->base_zone[stripe_group->base_start_idx + i].zone_id /
									detzone_ns->base_zsze;
				switch (detzone_ctrlr->zone_info[phy_zone_idx].state) {
				case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
				case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
				case SPDK_BDEV_ZONE_STATE_CLOSED:
				case SPDK_BDEV_ZONE_STATE_FULL:
					if (!(rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
									zone->base_zone[stripe_group->base_start_idx + i].zone_id,
									mgmt_io_ctx->zone_mgmt.zone_action, _detzone_zone_management_done, mgmt_io_ctx))) {
						mgmt_io_ctx->outstanding_mgmt_ios++;
					}
					break;
				default:
					break;
				}
				if (rc != 0) {
					break;
				}
			}
			if (rc != 0) {
				mgmt_io_ctx->remain_ios = 0;
				if (mgmt_io_ctx->outstanding_mgmt_ios != 0) {
					mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
					// error will be handled by completing IOs
					return;
				}
				mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_GENERIC;
				mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_INTERNAL_DEVICE_ERROR;
				goto complete;
			}
			zone->mgmt_in_progress = 1;
			/* fall through */
		default:
			// skip zones in other states
			mgmt_io_ctx->remain_ios -= 1;
			break;
		}
		zone_idx++;
	}

	if (mgmt_io_ctx->outstanding_mgmt_ios == 0) {
		mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
		goto complete;
	}
	return;

complete:
	_detzone_zone_management_complete(mgmt_io_ctx);
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

	zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id);
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

	if (mgmt_io_ctx->outstanding_mgmt_ios == 0) {
		mgmt_io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
		goto complete;
	}
	return;

complete:
	_detzone_zone_management_complete(mgmt_io_ctx);
	return;
	*/
}

#ifndef DETZONE_OVERDRIVE
static void
_vbdev_detzone_ns_shrink_zone_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone_ns_shrink_zone_ctx *ctx = cb_arg;
	struct vbdev_detzone_ns *detzone_ns = ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_zone_info *phy_zone;
	uint64_t zone_idx;

	if (success) {
		zone_idx = bdev_io->u.zone_mgmt.zone_id / detzone_ctrlr->mgmt_bdev.zone_size;
		phy_zone = &detzone_ctrlr->zone_info[zone_idx];
		switch (bdev_io->u.zone_mgmt.zone_action) {
		case SPDK_BDEV_ZONE_RESET:
			phy_zone->state = SPDK_BDEV_ZONE_STATE_EMPTY;
			phy_zone->write_pointer = phy_zone->zone_id;
			phy_zone->ns_id = 0;
			phy_zone->lzone_id = 0;
			phy_zone->stripe_id = 0;
			phy_zone->stripe_group = empty_stripe_group;
			phy_zone->pu_group = detzone_ctrlr->num_pu;
			TAILQ_INSERT_TAIL(&detzone_ctrlr->zone_empty, phy_zone, link);
			detzone_ctrlr->num_zone_empty += 1;
			break;
		case SPDK_BDEV_ZONE_CLOSE:
			detzone_ns->num_phy_open_zones -= 1;
			phy_zone->state = SPDK_BDEV_ZONE_STATE_CLOSED;
			break;
		default:
			assert(0);
		}
	}

	spdk_bdev_free_io(bdev_io);

	ctx->outstanding_ios -= 1;
	if (ctx->outstanding_ios) {
		return;
	}

	SPDK_DEBUGLOG(vbdev_detzone, "zone stats after shrink_complete: logical(%u/%u) phy actives(%u) phy opens(%u)\n",
										detzone_ns->num_open_zones, detzone_ns->num_active_zones,
										detzone_ns->num_phy_active_zones, detzone_ns->num_phy_open_zones);

	free(ctx);
}

static void
_vbdev_detzone_ns_shrink_zone_copy_done(void *arg)
{
	struct vbdev_detzone_ns_shrink_zone_ctx *ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	struct vbdev_detzone_ns_zone *zone;
	struct detzone_bdev_io *io_ctx, *tmp_ctx;
	uint64_t sgrp_idx;
	uint32_t base_start_idx, org_width;
	int rc;

	assert(ctx->org_write_pointer == ctx->org_zone->write_pointer);
	zone = ctx->org_zone;
	sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, ctx->org_write_pointer);
	stripe_group = &ctx->org_zone->stripe_group[sgrp_idx];
	org_width = stripe_group->width;
	base_start_idx = stripe_group->base_start_idx;
	// update zone info
	zone->num_zone_alloc = base_start_idx;
	for (uint32_t i = 0, idx = base_start_idx; i < org_width; ++i, ++idx) {
		if ((rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
				zone->base_zone[idx].zone_id,
				SPDK_BDEV_ZONE_RESET, _vbdev_detzone_ns_shrink_zone_complete, ctx))) {
			goto error;
		}
		//SPDK_DEBUGLOG(vbdev_detzone, "reset original phy_zone: org_width(%u) idx(%u) phy_zone_id(0x%lx)\n",
		//												 org_width, idx, zone->base_zone[idx].zone_id);
		ctx->outstanding_ios += 1;
		if (ctx->new_zone.stripe_group[i].width == 0) {
			zone->stripe_group[idx] = empty_stripe_group;
			zone->base_zone[idx].zone_id = UINT64_MAX;
		} else {
			zone->stripe_group[idx].base_start_idx = base_start_idx + ctx->new_zone.stripe_group[i].base_start_idx;
			zone->stripe_group[idx].slba = ctx->new_zone.stripe_group[i].slba;
			zone->stripe_group[idx].width = ctx->new_zone.stripe_group[i].width;
			zone->base_zone[idx].zone_id = ctx->new_zone.base_zone[i].zone_id;
			zone->num_zone_alloc += 1;
			/* SPDK_DEBUGLOG(vbdev_detzone, "update zoneinfo: zone_id(0x%lx) idx(%u) phy_zone_id(0x%lx) base_start_idx(%u) sgrp_slba(0x%lx) sgrp_width(%u)\n",
								ctx->org_zone->zone_id,
								idx,
								zone->base_zone[idx].zone_id,
								zone->stripe_group[idx].base_start_idx,
								zone->stripe_group[idx].slba,
								zone->stripe_group[idx].width); */
			if (zone->stripe_group[idx].slba != ctx->shrink_slba && !_detzone_is_open_state(zone->state)) {
				rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
						zone->base_zone[idx].zone_id,
						SPDK_BDEV_ZONE_CLOSE, _vbdev_detzone_ns_shrink_zone_complete, ctx);
				if (rc) {
					goto error;
				}
			}
			ctx->outstanding_ios += 1;
		}
	}

	SPDK_DEBUGLOG(vbdev_detzone, "Shrink completed: zone_id(0x%lx)\n", ctx->org_zone->zone_id);
	TAILQ_REMOVE(&detzone_ns->internal.active_zones, ctx->org_zone, active_link);
	TAILQ_INSERT_TAIL(&detzone_ns->internal.active_zones, ctx->org_zone, active_link);
	//_verify_active_zone_list(detzone_ns);
	detzone_ns->internal.total_shrinked_blks += ctx->org_write_pointer - ctx->shrink_slba;
	ctx->is_readable = true;
	ctx->is_writable = true;
	ctx->org_zone->shrink_ctx = NULL;
	spdk_thread_send_msg(detzone_ns->primary_thread, _detzone_ns_write_prepare, ctx->org_zone);
	TAILQ_FOREACH_SAFE(io_ctx, &detzone_ns->internal.rd_pending, link, tmp_ctx) {
		if (_vbdev_detzone_ns_get_zone_id(detzone_ns, io_ctx->u.io.next_offset_blocks) != zone->zone_id) {
			continue;
		}
		TAILQ_REMOVE(&detzone_ns->internal.rd_pending, io_ctx, link);
		_vbdev_detzone_ns_submit_request_read(spdk_bdev_io_from_ctx(io_ctx));
	}
	if (ctx->cb) {
		ctx->cb(ctx->cb_arg);
	}
	vbdev_detzone_reserve_zone(detzone_ctrlr);

	return;

error:
	SPDK_ERRLOG("Zone shrink failed (0x%lx). The zone is now in read-only\n", ctx->org_zone->zone_id);
	ctx->org_zone->state = SPDK_BDEV_ZONE_STATE_READ_ONLY;
	// TODO: need to reset zones and return them to the empty list
	assert(0);
	free(ctx);
	return;

}

static void
_vbdev_detzone_ns_shrink_zone_write_done(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone_ns_shrink_zone_ctx *ctx = cb_arg;
	struct vbdev_detzone_ns *detzone_ns = ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;

	if (!success) {
		ctx->org_zone->state = SPDK_BDEV_ZONE_STATE_READ_ONLY;
	} else {
		_vbdev_detzone_ns_forward_phy_zone_wp(detzone_ns, bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);
	}
	spdk_bdev_free_io(bdev_io);

	ctx->outstanding_ios--;
	if (ctx->outstanding_ios) {
		return;
	} else if (ctx->org_zone->state == SPDK_BDEV_ZONE_STATE_READ_ONLY) {
		goto error;
	}

	if ((ctx->next_copy_offset == ctx->org_write_pointer)
									 && ctx->num_packed_blks) {
		// copy is done for open zones
		// TODO: allow write IOs from this point
		SPDK_DEBUGLOG(vbdev_detzone, "copy for open zones complete\n");
		ctx->next_copy_offset = ctx->shrink_slba;
		vbdev_detzone_ns_shrink_zone_copy_start(ctx);
	} else if ((ctx->next_copy_offset == ctx->org_write_pointer)
				|| ((ctx->next_copy_offset - ctx->shrink_slba) == ctx->num_packed_blks)) {
		// copy is done
		SPDK_DEBUGLOG(vbdev_detzone, "Done copy for shrink (0x%lx -> 0x%lx)\n", ctx->next_copy_offset, ctx->org_write_pointer);
		ctx->is_readable = false;
		spdk_mempool_put(detzone_ns->io_buf_pool, ctx->io_buf);
		if (ctx->admitted_read_blks == 0) {
			spdk_thread_send_msg(detzone_ctrlr->thread, _vbdev_detzone_ns_shrink_zone_copy_done, ctx);
		} else {
			SPDK_DEBUGLOG(vbdev_detzone, "Pending shrink: wait for read IOs complete (0x%lx) blks(0x%lx)\n",
														 ctx->org_zone->zone_id, ctx->admitted_read_blks);
		}
	} else {
		vbdev_detzone_ns_shrink_zone_read(ctx);
	}
	return;

error:
	SPDK_ERRLOG("Zone shrink failed (0x%lx). The zone is now in read-only\n", ctx->org_zone->zone_id);
	ctx->org_zone->state = SPDK_BDEV_ZONE_STATE_READ_ONLY;
	spdk_mempool_put(detzone_ns->io_buf_pool, ctx->io_buf);
	// TODO: need to reset zones and return them to the empty list
	assert(0);
	free(ctx);
	return;
}

static void
_vbdev_detzone_ns_shrink_zone_write(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone_ns_shrink_zone_ctx *ctx = cb_arg;
	struct vbdev_detzone_ns *detzone_ns = ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct detzone_io_channel *detzone_ch;
	uint64_t sgrp_idx = (ctx->next_copy_offset - ctx->shrink_slba) / detzone_ns->base_avail_zcap;
	uint32_t stripe_width = ctx->new_zone.stripe_group[sgrp_idx].width;
	uint32_t base_start_idx = ctx->new_zone.stripe_group[sgrp_idx].base_start_idx;
	uint64_t num_blks, valid_blks, full_stripe_blks, remaining_blks;
	int rc;

	if (!success) {
		ctx->org_zone->state = SPDK_BDEV_ZONE_STATE_READ_ONLY;
	}
	spdk_bdev_free_io(bdev_io);

	assert(ctx->outstanding_ios);
	ctx->outstanding_ios -= 1;
	if (ctx->outstanding_ios) {
		return;
	} else if (ctx->org_zone->state == SPDK_BDEV_ZONE_STATE_READ_ONLY) {
		goto error;
	}

	valid_blks = spdk_min(ctx->org_write_pointer,
							 ctx->new_zone.stripe_group[sgrp_idx].slba + stripe_width * detzone_ns->base_avail_zcap)
							  - ctx->next_copy_offset;
	remaining_blks = valid_blks % (ctx->new_zone.stripe_group[sgrp_idx].stripe_blks * stripe_width);
	full_stripe_blks = valid_blks - remaining_blks;
	detzone_ch = spdk_io_channel_get_ctx(detzone_ns->primary_ch);
	for (uint32_t i = 0; i < stripe_width; i++) {
		if (valid_blks > ctx->max_copy_blks) {
			num_blks = ctx->max_copy_blks / stripe_width;
		} else {
			num_blks = 0;
			if (remaining_blks) {
				num_blks = spdk_min(ctx->new_zone.stripe_group[sgrp_idx].stripe_blks, remaining_blks);
				remaining_blks -= num_blks;
			}
			num_blks += full_stripe_blks / stripe_width;
		}

		if (num_blks == 0) {
			continue;
		}
		/* SPDK_DEBUGLOG(vbdev_detzone, "write: zone_id(0x%lx) offset(0x%lx) base_idx(%u) phy_offset(0x%lx) num_blks(0x%lx)\n",
										ctx->org_zone->zone_id,
										ctx->next_copy_offset,
										base_start_idx + i,
										_vbdev_detzone_ns_get_phy_zone_wp(detzone_ns, ctx->new_zone.base_zone[base_start_idx + i].zone_id),
										num_blks); */

		rc = spdk_bdev_writev_blocks(detzone_ctrlr->base_desc, detzone_ch->base_ch,
						ctx->new_zone.base_zone[base_start_idx + i].iovs, ctx->new_zone.base_zone[base_start_idx + i].iov_cnt,
						_vbdev_detzone_ns_get_phy_zone_wp(detzone_ns, ctx->new_zone.base_zone[base_start_idx + i].zone_id),
						num_blks, _vbdev_detzone_ns_shrink_zone_write_done, ctx);
		if (rc) {
			goto error;
		}
		ctx->next_copy_offset += num_blks;
		ctx->outstanding_ios += 1;
	}
	return;

error:

	SPDK_ERRLOG("Zone shrink failed (0x%lx). The zone is now in read-only\n", ctx->org_zone->zone_id);
	ctx->org_zone->state = SPDK_BDEV_ZONE_STATE_READ_ONLY;
	spdk_mempool_put(detzone_ns->io_buf_pool, ctx->io_buf);
	// TODO: need to reset zones and return them to the empty list
	assert(0);
	free(ctx);
	return;
}

static void
vbdev_detzone_ns_shrink_zone_read(void *arg)
{
	struct vbdev_detzone_ns_shrink_zone_ctx *ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_ns_stripe_group *org_stripe_group, *stripe_group;
	struct detzone_io_channel *detzone_ch;
	uint64_t sgrp_idx;
	uint64_t buf_offset_blks = 0;
	int rc;

	// now we've moved from mgmt_thread to primary_thread
	// thus, we should not touch the zone state metadata until we finish copy and switch to mgmt_thread
	sgrp_idx = (ctx->next_copy_offset - ctx->org_zone->zone_id) / detzone_ns->base_avail_zcap;
	org_stripe_group = &ctx->org_zone->stripe_group[sgrp_idx];
	sgrp_idx = (ctx->next_copy_offset - ctx->shrink_slba) / detzone_ns->base_avail_zcap;
	stripe_group = &ctx->new_zone.stripe_group[sgrp_idx];
	detzone_ch = spdk_io_channel_get_ctx(detzone_ns->primary_ch);
	while (buf_offset_blks < ctx->max_copy_blks) {
		void *buf_ptr;
		uint64_t phy_offset_blks, num_blks;
		phy_offset_blks = _vbdev_detzone_ns_get_phy_offset(detzone_ns, ctx->next_copy_offset + buf_offset_blks);
		num_blks = spdk_min(ctx->org_write_pointer - (ctx->next_copy_offset + buf_offset_blks), org_stripe_group->stripe_blks);
		buf_ptr = (char*)ctx->io_buf + (buf_offset_blks * detzone_ns->detzone_ns_bdev.blocklen);
		/* SPDK_DEBUGLOG(vbdev_detzone, "read: zone_id(0x%lx) buf_ptr(0x%p) offset(0x%lx) phy_offset(0x%lx) num_blks(0x%lx)\n",
										ctx->org_zone->zone_id, buf_ptr, ctx->next_copy_offset + buf_offset_blks,
										phy_offset_blks, num_blks); */
		rc = spdk_bdev_read_blocks(detzone_ctrlr->base_desc, detzone_ch->base_ch,
						buf_ptr,
						phy_offset_blks, num_blks, _vbdev_detzone_ns_shrink_zone_write,
						ctx);
		if (rc) {
			goto error;
		}
		ctx->outstanding_ios += 1;
		buf_offset_blks += num_blks;
		if (ctx->next_copy_offset + buf_offset_blks == ctx->org_write_pointer
			|| ctx->next_copy_offset + buf_offset_blks
				  == stripe_group->slba + stripe_group->width * detzone_ns->base_avail_zcap) {
			break;
		}
	}

	return;

error:
	SPDK_ERRLOG("Zone shrink failed (0x%lx). The zone is now in read-only\n", ctx->org_zone->zone_id);
	ctx->org_zone->state = SPDK_BDEV_ZONE_STATE_READ_ONLY;
	spdk_mempool_put(detzone_ns->io_buf_pool, ctx->io_buf);
	// TODO: need to reset zones and return them to the empty list
	assert(0);
	free(ctx);
	return;
}

static void
vbdev_detzone_ns_shrink_zone_copy_start(void *arg)
{
	struct vbdev_detzone_ns_shrink_zone_ctx *ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	uint64_t sgrp_idx = (ctx->next_copy_offset - ctx->shrink_slba) / detzone_ns->base_avail_zcap;
	// setup buf and iovs for packed_zones
	uint64_t buf_offset_blks = 0;
	uint32_t stripe_width = ctx->new_zone.stripe_group[sgrp_idx].width;
	uint32_t stripe_blks = ctx->new_zone.stripe_group[sgrp_idx].stripe_blks;
	uint32_t base_start_idx = ctx->new_zone.stripe_group[sgrp_idx].base_start_idx;
	uint32_t basezone_idx = base_start_idx;
	int iov_idx = 0;

	if (!ctx->io_buf) {
		ctx->io_buf = spdk_mempool_get(detzone_ns->io_buf_pool);
	}
	assert(ctx->io_buf);
	ctx->max_copy_blks = detzone_ctrlr->per_zone_mdts * stripe_width;

	while (buf_offset_blks < ctx->max_copy_blks) {
		ctx->new_zone.base_zone[basezone_idx].iovs[iov_idx].iov_base = (char*)ctx->io_buf
											+ (buf_offset_blks * detzone_ns->detzone_ns_bdev.blocklen);
		ctx->new_zone.base_zone[basezone_idx].iovs[iov_idx].iov_len = stripe_blks
											* detzone_ns->detzone_ns_bdev.blocklen;
		ctx->new_zone.base_zone[basezone_idx].iov_cnt += 1;
		/* SPDK_DEBUGLOG(vbdev_detzone, "setup iov: zone_id(0x%lx) base_idx(%u) iov_idx(%u) iov_cnt(%u) bufaddr(0x%p) len(0x%lx)\n",
									ctx->org_zone->zone_id, basezone_idx,
									iov_idx, ctx->new_zone.base_zone[basezone_idx].iov_cnt,
									ctx->new_zone.base_zone[basezone_idx].iovs[iov_idx].iov_base,
									ctx->new_zone.base_zone[basezone_idx].iovs[iov_idx].iov_len); */
		buf_offset_blks += stripe_blks;
		basezone_idx += 1;
		if (basezone_idx - base_start_idx == stripe_width) {
			basezone_idx = base_start_idx;
			iov_idx += 1;
		}
	}
	vbdev_detzone_ns_shrink_zone_read(ctx);
}

static void
_vbdev_detzone_ns_shrink_zone_alloc_done(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone_ns_shrink_zone_ctx *ctx = cb_arg;
	struct vbdev_detzone_ns *detzone_ns = ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_zone_info *phy_zone;
	uint64_t zone_idx;

	if (!success) {
		ctx->org_zone->state = SPDK_BDEV_ZONE_STATE_READ_ONLY;
	} else {
		_vbdev_detzone_ns_forward_phy_zone_wp(detzone_ns, bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);
	}
	spdk_bdev_free_io(bdev_io);

	ctx->outstanding_ios--;
	if (ctx->outstanding_ios) {
		return;
	}
	spdk_mempool_put(detzone_ns->md_buf_pool, ctx->md_buf);
	if (ctx->org_zone->state != SPDK_BDEV_ZONE_STATE_READ_ONLY) {
		SPDK_DEBUGLOG(vbdev_detzone, "zone stats after shrink_alloc: logical(%u/%u) phy actives(%u) phy opens(%u)\n",
											detzone_ns->num_open_zones, detzone_ns->num_active_zones,
											detzone_ns->num_phy_active_zones, detzone_ns->num_phy_open_zones);
		if (detzone_ns->primary_thread != spdk_get_thread()) {
			spdk_thread_send_msg(detzone_ns->primary_thread, vbdev_detzone_ns_shrink_zone_copy_start, ctx);
		} else {
			vbdev_detzone_ns_shrink_zone_copy_start(ctx);
		}
	} else {
		SPDK_ERRLOG("Zone shrink failed (0x%lx). The zone is now in read-only\n", ctx->org_zone->zone_id);
		for (uint32_t i = 0; i < ctx->new_zone.num_zone_alloc; i++) {
			if (ctx->new_zone.base_zone[i].zone_id != UINT64_MAX) {
				zone_idx = ctx->new_zone.base_zone[i].zone_id / detzone_ns->base_zsze;
				phy_zone = &detzone_ctrlr->zone_info[zone_idx];
				TAILQ_INSERT_TAIL(&detzone_ctrlr->zone_reserved, phy_zone, link);
				detzone_ctrlr->num_zone_reserved++;
			}
		}
		free(ctx);
	}

	return;
}

static void
vbdev_detzone_ns_shrink_zone_alloc(void *arg)
{
	struct vbdev_detzone_ns_shrink_zone_ctx *ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	struct vbdev_detzone_zone_info *phy_zone;
	struct vbdev_detzone_zone_md *zone_md;
	uint64_t sgrp_idx;
	uint32_t packed_width, open_width;
	uint32_t i;
	int rc;

	if (ctx == TAILQ_FIRST(&detzone_ctrlr->internal.zone_shrink_queued)) {
		TAILQ_REMOVE(&detzone_ctrlr->internal.zone_shrink_queued, ctx, link);
	} else if (!TAILQ_EMPTY(&detzone_ctrlr->internal.zone_shrink_queued)) {
		SPDK_DEBUGLOG(vbdev_detzone, "queued shrink: zone_id(0x%lx)\n", ctx->org_zone->zone_id);
		TAILQ_INSERT_TAIL(&detzone_ctrlr->internal.zone_shrink_queued, ctx, link);
		return;
	}

	sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, ctx->org_write_pointer);
	stripe_group = &ctx->org_zone->stripe_group[sgrp_idx];
	packed_width = ctx->num_packed_blks / detzone_ns->base_avail_zcap;
	open_width = ctx->num_alloc_require - packed_width;
	ctx->md_buf = spdk_mempool_get(detzone_ns->md_buf_pool);
	for (i = 0; i < ctx->num_alloc_require; i++) {
		phy_zone = _vbdev_detzone_ns_get_phyzone(detzone_ns);
		ctx->new_zone.base_zone[i].zone_id = phy_zone->zone_id;
		if (i < packed_width) {
			ctx->new_zone.stripe_group[i].slba = stripe_group->slba;
			ctx->new_zone.stripe_group[i].base_start_idx = 0;
			ctx->new_zone.stripe_group[i].width = packed_width;
		} else {
			ctx->new_zone.stripe_group[i].slba = stripe_group->slba + ctx->num_packed_blks;
			ctx->new_zone.stripe_group[i].base_start_idx = packed_width;
			ctx->new_zone.stripe_group[i].width = open_width;
		}
		/* SPDK_DEBUGLOG(vbdev_detzone, "new phy_zone: zone_id(0x%lx) idx(%u) base_idx(%u) sgrp_width(%u) sgrp_slba(0x%lx) phy_zone_id(0x%lx)\n",
									ctx->org_zone->zone_id, i,
									ctx->new_zone.stripe_group[i].base_start_idx,
									ctx->new_zone.stripe_group[i].width,
									ctx->new_zone.stripe_group[i].slba,
									phy_zone->zone_id); */
		
		phy_zone->ns_id = detzone_ns->nsid;
		phy_zone->lzone_id = ctx->org_zone->zone_id;
		phy_zone->stripe_id = stripe_group->base_start_idx + i;
		phy_zone->stripe_group = ctx->new_zone.stripe_group[i];

		zone_md = (struct vbdev_detzone_zone_md *)((uint8_t*)ctx->md_buf
								+ (i * detzone_ns->padding_blocks * detzone_ns->detzone_ns_bdev.blocklen));
		zone_md->version = DETZONE_NS_META_FORMAT_VER;
		zone_md->lzone_id = phy_zone->lzone_id;
		zone_md->ns_id = phy_zone->ns_id;
		zone_md->stripe_id = phy_zone->stripe_id;
		zone_md->stripe_group = phy_zone->stripe_group;
		// write metadata
		rc = spdk_bdev_write_blocks(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
						zone_md,
						phy_zone->zone_id + DETZONE_RESERVATION_BLKS,
						detzone_ns->padding_blocks - DETZONE_RESERVATION_BLKS,
						_vbdev_detzone_ns_shrink_zone_alloc_done, ctx);
		if (rc) {
			goto error;
		}
		ctx->outstanding_ios += 1;
		ctx->new_zone.num_zone_alloc += 1;
	}

	return;

error:
	SPDK_ERRLOG("Zone shrink failed (0x%lx). The zone is now in read-only\n", ctx->org_zone->zone_id);
	ctx->org_zone->state = SPDK_BDEV_ZONE_STATE_READ_ONLY;
	spdk_mempool_put(detzone_ns->md_buf_pool, ctx->md_buf);
	// TODO: reset and return zones to the empty list
	assert(0);
	free(ctx);

}

static void
_vbdev_detzone_ns_shrink_zone_finish_done(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone_ns_shrink_zone_ctx *ctx = cb_arg;
	struct vbdev_detzone_ns *detzone_ns = ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_zone_info *phy_zone;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	uint64_t valid_blks, sgrp_idx, zone_idx;
	uint32_t packed_width, open_width = 0;
	uint32_t phy_zone_quota;
	uint32_t i;

	if (!success) {
		ctx->org_zone->state = SPDK_BDEV_ZONE_STATE_READ_ONLY;
	}
	spdk_bdev_free_io(bdev_io);

	ctx->outstanding_ios--;
	if (ctx->outstanding_ios) {
		return;
	} else if (ctx->org_zone->state == SPDK_BDEV_ZONE_STATE_READ_ONLY) {
		goto error;
	}

	sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, ctx->org_write_pointer);
	stripe_group = &ctx->org_zone->stripe_group[sgrp_idx];
	valid_blks = ctx->org_write_pointer - stripe_group->slba;
	phy_zone_quota = (detzone_ctrlr->mgmt_bdev.max_active_zones - DETZONE_MAX_RESERVE_ZONES) / detzone_ctrlr->num_ns;
	
	//open_width = phy_zone_quota / detzone_ns->detzone_ns_bdev.max_active_zones;
	//packed_width = (valid_blks - (valid_blks % (open_width * detzone_ns->base_avail_zcap))) / detzone_ns->base_avail_zcap;

	if (_detzone_is_open_state(ctx->org_zone->state)) {
		open_width = phy_zone_quota / detzone_ns->detzone_ns_bdev.max_active_zones;
	} else {
		open_width = phy_zone_quota / detzone_ns->detzone_ns_bdev.max_active_zones / 2;
	}
	open_width = spdk_max(1, open_width);
	packed_width = spdk_min((valid_blks - (valid_blks % detzone_ns->base_avail_zcap)) / detzone_ns->base_avail_zcap,
								stripe_group->width - open_width);

	assert(open_width + packed_width <= stripe_group->width);

	ctx->num_packed_blks = packed_width * detzone_ns->base_avail_zcap;
	ctx->next_copy_offset = ctx->shrink_slba + ctx->num_packed_blks;

	for (i = 0; i < stripe_group->width; i++) {
		zone_idx = ctx->org_zone->base_zone[stripe_group->base_start_idx + i].zone_id
							 / detzone_ns->base_zsze;
		phy_zone = &detzone_ctrlr->zone_info[zone_idx];
		if (_detzone_is_active_state(phy_zone->state)) {
			_vbdev_detzone_release_phy_actives(detzone_ctrlr);
			detzone_ns->num_phy_active_zones -= 1;
		}
		if (_detzone_is_open_state(phy_zone->state)) {
			detzone_ns->num_phy_open_zones -= 1;
		}
		phy_zone->state = SPDK_BDEV_ZONE_STATE_FULL;
	}

	SPDK_DEBUGLOG(vbdev_detzone, "Finish victim zone complete: zone_id(0x%lx)\n", ctx->org_zone->zone_id);
	ctx->num_alloc_require = packed_width + open_width;
	if (ctx->num_alloc_require > detzone_ctrlr->num_zone_reserved) {
		TAILQ_INSERT_TAIL(&detzone_ctrlr->internal.zone_shrink_queued, ctx, link);
		SPDK_DEBUGLOG(vbdev_detzone, "Need more rsvd: zone_id(0x%lx) req(%u) rsvd(%u)\n",
											 ctx->org_zone->zone_id,
											 ctx->num_alloc_require, detzone_ctrlr->num_zone_reserved);
		vbdev_detzone_reserve_zone(detzone_ctrlr);
	} else {
		vbdev_detzone_ns_shrink_zone_alloc(ctx);
	}

	return;

error:
	SPDK_ERRLOG("Zone shrink failed (0x%lx). The zone is now in read-only\n", ctx->org_zone->zone_id);
	ctx->org_zone->state = SPDK_BDEV_ZONE_STATE_READ_ONLY;
	spdk_mempool_put(detzone_ns->md_buf_pool, ctx->md_buf);
	// TODO: reset and return zones to the empty list
	assert(0);
	free(ctx);
}

static void
vbdev_detzone_ns_shrink_zone_submit(void *arg)
{
	struct vbdev_detzone_ns_shrink_zone_ctx *ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	struct vbdev_detzone_ns_zone *zone;
	struct detzone_bdev_io *io_ctx, *tmp_ctx;	
	uint64_t sgrp_idx;
	uint32_t i;
	int rc;

	zone = ctx->org_zone;
	sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, zone->write_pointer);
	stripe_group = &zone->stripe_group[sgrp_idx];

	// In case of shrink following outstanding write IOs,
	// we need to check if this zone is still effective.
	if (zone->write_pointer != ctx->org_write_pointer) {
		if (stripe_group->slba != ctx->shrink_slba) {
			SPDK_DEBUGLOG(vbdev_detzone, "Cancel shrink: zone(0x%lx) has been assigned to new stripe group\n",
									zone->zone_id);
			ctx->is_readable = true;
			ctx->is_writable = true;
			ctx->org_zone->shrink_ctx = NULL;
			spdk_thread_send_msg(detzone_ns->primary_thread, _detzone_ns_write_prepare, ctx->org_zone);
			TAILQ_FOREACH_SAFE(io_ctx, &detzone_ns->internal.rd_pending, link, tmp_ctx) {
				if (_vbdev_detzone_ns_get_zone_id(detzone_ns, io_ctx->u.io.next_offset_blocks) != zone->zone_id) {
					continue;
				}
				TAILQ_REMOVE(&detzone_ns->internal.rd_pending, io_ctx, link);
				_vbdev_detzone_ns_submit_request_read(spdk_bdev_io_from_ctx(io_ctx));
			}
			if (ctx->cb) {
				ctx->cb(ctx->cb_arg);
			}
			vbdev_detzone_reserve_zone(detzone_ctrlr);
			free(ctx);
			return;
		}
		ctx->org_write_pointer = zone->write_pointer;
	}

	for (i = 0; i < stripe_group->width; i++) {
		rc = spdk_bdev_zone_management(detzone_ctrlr->base_desc, detzone_ctrlr->mgmt_ch,
				ctx->org_zone->base_zone[stripe_group->base_start_idx + i].zone_id,
				SPDK_BDEV_ZONE_FINISH, _vbdev_detzone_ns_shrink_zone_finish_done, ctx);
		if (rc) {
			goto error;
		}
		ctx->outstanding_ios += 1;
	}

	//spdk_thread_send_msg(detzone_ctrlr->thread, vbdev_detzone_reserve_zone, detzone_ctrlr);
	return;

error:
	SPDK_ERRLOG("Zone shrink failed (0x%lx). The zone is now in read-only\n", ctx->org_zone->zone_id);
	ctx->org_zone->state = SPDK_BDEV_ZONE_STATE_READ_ONLY;
	// TODO: reset and return zones to the empty list
	assert(0);
	free(ctx);
}

static void
vbdev_detzone_ns_shrink_zone(struct vbdev_detzone_ns *detzone_ns, uint64_t zone_id, void *arg)
{
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_ns_zone *zone;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	struct vbdev_detzone_ns_shrink_zone_ctx *ctx;
	uint64_t zone_idx, sgrp_idx, valid_blks;
	uint32_t packed_width, open_width, phy_zone_quota;

	zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, zone_id);
	zone = &detzone_ns->internal.zone[zone_idx];
	sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, zone->write_pointer);
	stripe_group = &zone->stripe_group[sgrp_idx];
	valid_blks = zone->write_pointer - stripe_group->slba;
	phy_zone_quota = (detzone_ctrlr->mgmt_bdev.max_active_zones - DETZONE_MAX_RESERVE_ZONES) / detzone_ctrlr->num_ns;

	if (_detzone_is_open_state(zone->state)) {
		open_width = phy_zone_quota / detzone_ns->detzone_ns_bdev.max_active_zones;
	} else {
		open_width = phy_zone_quota / detzone_ns->detzone_ns_bdev.max_active_zones / 2;
	}
	open_width = spdk_max(1, open_width);
	packed_width = spdk_min((valid_blks - (valid_blks % detzone_ns->base_avail_zcap)) / detzone_ns->base_avail_zcap,
								stripe_group->width - open_width);
	
	//packed_width = (valid_blks - (valid_blks % (open_width * detzone_ns->base_avail_zcap))) / detzone_ns->base_avail_zcap;
	assert(open_width + packed_width <= stripe_group->width);

	SPDK_DEBUGLOG(vbdev_detzone, "Shrink sparse zone: zone_id(0x%lx) width(%u) wp(0x%lx) packed(%u) open(%u)\n",
											zone->zone_id, stripe_group->width, zone->write_pointer, packed_width, open_width);
	ctx = calloc(sizeof(struct vbdev_detzone_ns_shrink_zone_ctx), 1);
	ctx->detzone_ns = detzone_ns;
	ctx->org_zone = zone;
	ctx->org_write_pointer = zone->write_pointer;
	ctx->shrink_slba = stripe_group->slba;
	ctx->is_writable = false;
	ctx->is_readable = true;
	zone->shrink_ctx = ctx;
	// TODO: Actually, we have to iterate all io channels to ensure that there is no outstanding IOs in the zone.
	// If any, we need to wait until they complete, then initiate the shrink.
	if (zone->write_pending_blks == 0) {
		spdk_thread_send_msg(detzone_ctrlr->thread, vbdev_detzone_ns_shrink_zone_submit, zone->shrink_ctx);
	} else {
		SPDK_DEBUGLOG(vbdev_detzone, "There is outstanding write IOs. This job will resume by them\n");
	}
	return;
}

static void
_detzone_ns_io_read_complete_shrink(void *arg)
{
	struct detzone_bdev_io *io_ctx = arg;
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(io_ctx);
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns,
															 detzone_ns_bdev);
	struct vbdev_detzone_ns_shrink_zone_ctx *shrink_ctx;

	shrink_ctx = _vbdev_detzone_ns_get_zone_is_shrink(detzone_ns, bdev_io->u.bdev.offset_blocks, false);
	if (spdk_unlikely(shrink_ctx != NULL)) {
		shrink_ctx->admitted_read_blks -= bdev_io->u.bdev.num_blocks;
		/*SPDK_DEBUGLOG(vbdev_detzone, "Read in shrink done: zone(0x%lx) offset(0x%lx) blks(0x%lx) remains(0x%lx)\n",
												shrink_ctx->org_zone->zone_id,
												bdev_io->u.bdev.offset_blocks,
												bdev_io->u.bdev.num_blocks,
												shrink_ctx->admitted_read_blks); */
		if (shrink_ctx->admitted_read_blks == 0 && !shrink_ctx->is_readable) {
			_vbdev_detzone_ns_shrink_zone_copy_done(shrink_ctx);
		}
	}
	_detzone_ns_io_complete(io_ctx);
}

static void
_vbdev_detzone_ns_submit_request_read_shrink(void *arg)
{
	struct spdk_bdev_io *bdev_io = arg;
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	struct vbdev_detzone_ns *detzone_ns = spdk_io_channel_get_io_device(spdk_bdev_io_get_io_channel(bdev_io));
	struct vbdev_detzone_ns_shrink_zone_ctx *shrink_ctx;

	shrink_ctx = _vbdev_detzone_ns_get_zone_is_shrink(detzone_ns, bdev_io->u.bdev.offset_blocks, false);
	if (spdk_unlikely(shrink_ctx != NULL)) {
		if (shrink_ctx->is_readable) {
			io_ctx->in_shrink = true;
			shrink_ctx->admitted_read_blks += bdev_io->u.bdev.num_blocks;
		} else {
			TAILQ_INSERT_TAIL(&detzone_ns->internal.rd_pending, io_ctx, link);
			SPDK_DEBUGLOG(vbdev_detzone, "READ in shrinking: offset(0x%lx), blks(0x%lx) \n",
											io_ctx->u.io.next_offset_blocks,
											bdev_io->u.bdev.num_blocks);
			return;
		}
	}

	_vbdev_detzone_ns_submit_request_read(bdev_io);
}
#endif

/*
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
*/

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

	zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, zslba);
	num_lzones = detzone_ns->detzone_ns_bdev.blockcnt / detzone_ns->detzone_ns_bdev.zone_size;

	for (i=0; i < num_zones && zone_idx + i < num_lzones; i++) {
		info[i].zone_id = _vbdev_detzone_ns_get_zone_id_by_idx(detzone_ns, zone_idx + i);;
		info[i].write_pointer = _vbdev_detzone_ns_get_zone_wp(detzone_ns, info[i].zone_id);
		info[i].capacity = _vbdev_detzone_ns_get_zone_cap(detzone_ns, info[i].zone_id);
		info[i].state = _vbdev_detzone_ns_get_zone_state(detzone_ns, info[i].zone_id);
	}
	return 0;
}

static void
_detzone_ns_zone_management_submit(void *arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx = arg;
	struct vbdev_detzone_ns *detzone_ns = mgmt_io_ctx->detzone_ns;
	struct vbdev_detzone *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	struct vbdev_detzone_ns_shrink_zone_ctx *shrink_ctx = NULL;

	if (detzone_ctrlr->thread != spdk_get_thread()) {
		spdk_thread_send_msg(detzone_ctrlr->thread, _detzone_ns_zone_management_submit, arg);
		return;
	}

#ifndef DETZONE_OVERDRIVE
	shrink_ctx = _vbdev_detzone_ns_get_zone_is_shrink(detzone_ns, mgmt_io_ctx->zone_mgmt.zone_id, mgmt_io_ctx->zone_mgmt.select_all);
	if (shrink_ctx) {
		SPDK_DEBUGLOG(vbdev_detzone, "a shrink zone is pending. queue MGMT_IO action(%s) zone_id(0x%lx)\n",
										action_str[mgmt_io_ctx->zone_mgmt.zone_action],
										mgmt_io_ctx->zone_mgmt.zone_id);
		if (shrink_ctx->cb) {
			SPDK_ERRLOG("another shrink callback was previsouly registered.");
			mgmt_io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			mgmt_io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_INVALID_STATE;
			_detzone_zone_management_complete(mgmt_io_ctx);
		} else {
			shrink_ctx->cb = _detzone_ns_zone_management_submit;
			shrink_ctx->cb_arg = mgmt_io_ctx;
		}
		return;
	}
#endif
	// SPDK_DEBUGLOG(vbdev_detzone,
	// 					 "Zone management: action(%s) select_all(%d) implicit(%d) ns_id(%u) logi_zone_id(0x%lx)\n",
	// 					 action_str[mgmt_io_ctx->zone_mgmt.zone_action],
	// 					 mgmt_io_ctx->zone_mgmt.select_all,
	// 					 mgmt_io_ctx->parent_io->type == SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT ? 0:1,
	// 					 detzone_ns->nsid, mgmt_io_ctx->zone_mgmt.zone_id);

	switch (mgmt_io_ctx->zone_mgmt.zone_action) {
	case SPDK_BDEV_ZONE_CLOSE:
		vbdev_detzone_ns_close_zone(mgmt_io_ctx);
		break;
	case SPDK_BDEV_ZONE_FINISH:
		vbdev_detzone_ns_finish_zone(mgmt_io_ctx);
		break;
	case SPDK_BDEV_ZONE_OPEN:
		if (mgmt_io_ctx->parent_io->type == SPDK_BDEV_IO_TYPE_ZONE_MANAGEMENT) {
			vbdev_detzone_ns_open_zone(mgmt_io_ctx);
		} else {
			assert(mgmt_io_ctx->submitted_thread == detzone_ns->primary_thread);
			vbdev_detzone_ns_imp_open_zone(mgmt_io_ctx);
		}
		break;
	case SPDK_BDEV_ZONE_RESET:
		vbdev_detzone_ns_reset_zone(mgmt_io_ctx);
		break;
	case SPDK_BDEV_ZONE_OFFLINE:
		vbdev_detzone_ns_offline_zone(mgmt_io_ctx);
		break;
	case SPDK_BDEV_ZONE_SET_ZDE:
	default:
		assert(0);
	}
	return;
}

static int
_detzone_ns_zone_management(struct vbdev_detzone_ns *detzone_ns, struct spdk_bdev_io *bdev_io,
							  uint64_t lzslba, bool sel_all, enum spdk_bdev_zone_action action,
							  detzone_ns_mgmt_completion_cb cb, void *cb_arg)
{
	struct vbdev_detzone_ns_mgmt_io_ctx *mgmt_io_ctx;

	if (!sel_all) {
		if (lzslba >= detzone_ns->detzone_ns_bdev.blockcnt) {
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
	mgmt_io_ctx->submitted_thread = spdk_get_thread();
	mgmt_io_ctx->detzone_ns = detzone_ns;
	mgmt_io_ctx->remain_ios = mgmt_io_ctx->zone_mgmt.num_zones;
	mgmt_io_ctx->outstanding_mgmt_ios = 0;
	mgmt_io_ctx->cb = cb;
	mgmt_io_ctx->cb_arg = cb_arg;

	_detzone_ns_zone_management_submit(mgmt_io_ctx);
	return 0;
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

	// SPDK_DEBUGLOG(vbdev_detzone, "COMPLETE: type(%d) lba(0x%lx) num(0x%lx)\n",
	// 								io_ctx->type, bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);

	if (spdk_get_thread() != spdk_bdev_io_get_thread(bdev_io)) {
		spdk_thread_send_msg(spdk_bdev_io_get_thread(bdev_io), _detzone_ns_io_complete, io_ctx);
		return;
	}

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
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(orig_io->bdev, struct vbdev_detzone_ns,
					 detzone_ns_bdev);
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)orig_io->driver_ctx;
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(io_ctx->ch);
	struct vbdev_detzone_zone_info *phy_zone;
	struct vbdev_detzone_ns_zone *zone;
	struct detzone_io_channel_cong *zone_cong;
	uint64_t io_latency_tsc;
	uint64_t sgrp_idx;
	bool is_congested = false;

	phy_zone = &detzone_ctrlr->zone_info[bdev_io->u.bdev.offset_blocks / detzone_ns->base_zsze];
	assert(phy_zone->stripe_group.slba != UINT64_MAX);
	sgrp_idx = phy_zone->stripe_group.base_start_idx;
	assert(sgrp_idx < DETZONE_MAX_STRIPE_WIDTH);
	io_latency_tsc = (spdk_get_ticks() - spdk_bdev_io_get_submit_tsc(bdev_io));
	zone_cong = &detzone_ch->zone_cong[_vbdev_detzone_ns_get_zone_idx(detzone_ns, phy_zone->lzone_id)];
	assert(_vbdev_detzone_ns_get_zone_idx(detzone_ns, phy_zone->lzone_id) == _vbdev_detzone_ns_get_zone_idx(detzone_ns, io_ctx->u.io.next_offset_blocks));
	if (!success) {
		io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
		spdk_bdev_io_get_nvme_status(bdev_io, &io_ctx->nvme_status.cdw0, &io_ctx->nvme_status.sct, &io_ctx->nvme_status.sc);
		SPDK_ERRLOG("Partial I/O has failed: sct(0x%2x) sc(0x%2x)\n", io_ctx->nvme_status.sct, io_ctx->nvme_status.sc);
	} else {
		// cognestion detect
		//if (io_latency_tsc > detzone_ch->rd_lat_tsc_thresh << 1) {
		if (io_latency_tsc > detzone_ch->rd_lat_tsc_thresh) {
			// this is congestion signal
			is_congested = true;
		} else {
			// update per channel latency statistics (only non-congested lat)
			//SPDK_DEBUGLOG(vbdev_detzone, "read optimal: cwnd(%u) outstandings(%u) update_delay(%u) cpls(%u)\n",
			//									cong->cwnd, cong->outstandings, cong->cwnd_update_delay, cong->cpls);
		}	
		//detzone_ch->rd_lat_tsc_thresh = DETZONE_EWMA(detzone_ch->rd_lat_tsc_thresh, io_latency_tsc);
	}
	// SPDK_DEBUGLOG(vbdev_detzone, "read IO done : read (0x%lx) curr(%lu) outs(%u) remaing(0x%lx)\n",
	// 								io_ctx->u.io.next_offset_blocks,
	// 								sgrp_idx,
	// 								io_ctx->u.io.outstanding_stripe_ios,
	// 								io_ctx->u.io.remain_blocks);
	spdk_bdev_free_io(bdev_io);

	assert(io_ctx->u.io.outstanding_stripe_ios);
	io_ctx->u.io.outstanding_stripe_ios--;

	if (io_ctx->u.io.outstanding_stripe_ios == 0 && io_ctx->u.io.remain_blocks == 0) {
		if (io_ctx->status != SPDK_BDEV_IO_STATUS_FAILED) {
			io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
		}
#ifdef DETZONE_OVERDRIVE
		// SPDK_DEBUGLOG(vbdev_detzone, "read (0x%lx) complete: cwnd(%u) outstandings(%u) pending(%s)\n", io_ctx->u.io.next_offset_blocks,
		// 									zone_cong->sgrp[sgrp_idx].cwnd,
		// 									zone_cong->sgrp[sgrp_idx].outstandings,
		// 									TAILQ_FIRST(&zone_cong->sgrp[sgrp_idx].pending) ? "YES" : "NO");
		_detzone_ns_io_complete(io_ctx);
#else
		if (spdk_unlikely(io_ctx->in_shrink)) {
			spdk_thread_send_msg(detzone_ctrlr->thread, _detzone_ns_io_read_complete_shrink, io_ctx);
		} else {
			_detzone_ns_io_complete(io_ctx);
		}
#endif
	}

	assert(zone_cong->sgrp[sgrp_idx].outstandings > 0);
	zone_cong->sgrp[sgrp_idx].outstandings -= 1;
	if (zone_cong->sgrp[sgrp_idx].cwnd_update_delay) {
		zone_cong->sgrp[sgrp_idx].cwnd_update_delay -= 1;
	}

	if (is_congested && zone_cong->sgrp[sgrp_idx].cwnd_update_delay == 0) {
/* 		//if (1) {
		if (io_latency_tsc > detzone_ch->rd_lat_tsc_thresh << 2) {
			fprintf(stderr, "read congestion: ns(%u) (%llu / %llu us): sgrp_slba(0x%lx) cwnd(%u) outstandings(%u) update_delay(%u) cpls(%u)\n",
											detzone_ns->nsid,
											io_latency_tsc * SPDK_SEC_TO_USEC / spdk_get_ticks_hz(),
											detzone_ch->rd_lat_tsc_thresh * SPDK_SEC_TO_USEC / spdk_get_ticks_hz(),
											phy_zone->stripe_group.slba,
											zone_cong->sgrp[sgrp_idx].cwnd,
											zone_cong->sgrp[sgrp_idx].outstandings,
											zone_cong->sgrp[sgrp_idx].cwnd_update_delay,
											zone_cong->sgrp[sgrp_idx].cpls);
		} */
		zone_cong->sgrp[sgrp_idx].cwnd_update_delay = zone_cong->sgrp[sgrp_idx].cwnd;
		zone_cong->sgrp[sgrp_idx].cpls = 0;

		zone_cong->sgrp[sgrp_idx].cwnd = spdk_max(phy_zone->stripe_group.width,
							 zone_cong->sgrp[sgrp_idx].cwnd * (detzone_ch->rd_lat_tsc_thresh >> 1) / io_latency_tsc);
		//zone_cong->sgrp[sgrp_idx].cwnd = spdk_max(zone_cong->sgrp[sgrp_idx].cwnd >> 1, phy_zone->stripe_group.width);
		//zone_cong->sgrp[sgrp_idx].cwnd = phy_zone->stripe_group.width;

	} else if (!is_congested) {
		zone_cong->sgrp[sgrp_idx].cpls += 1;
		if (zone_cong->sgrp[sgrp_idx].cpls >= zone_cong->sgrp[sgrp_idx].cwnd << 3) {
			if (zone_cong->sgrp[sgrp_idx].cwnd < phy_zone->stripe_group.width << 2) {
			//if (zone_cong->sgrp[sgrp_idx].cwnd < detzone_ctrlr->per_zone_mdts << spdk_u32log2(phy_zone->stripe_group.width)) {
				zone_cong->sgrp[sgrp_idx].cwnd += 1;
			}
			zone_cong->sgrp[sgrp_idx].cpls = 0;
		}

	}
	//zone_cong->sgrp[sgrp_idx].cwnd = phy_zone->stripe_group.width;
	//zone_cong->sgrp[sgrp_idx].cwnd = UINT32_MAX;

	// check if there is next IOs to submit
	// the thread can be released iff the pending queue is empty and no outstandings
	io_ctx = TAILQ_FIRST(&zone_cong->sgrp[sgrp_idx].pending);
	if (io_ctx && zone_cong->sgrp[sgrp_idx].cwnd > zone_cong->sgrp[sgrp_idx].outstandings) {
		assert(io_ctx->status == SPDK_BDEV_IO_STATUS_PENDING);
		_detzone_ns_io_read_submit(io_ctx);
	}
	return;
}

static void
_detzone_ns_io_read_submit(void *arg)
{
	struct detzone_bdev_io *io_ctx = arg;
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(io_ctx);
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns,
					 detzone_ns_bdev);
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(io_ctx->ch);
	struct vbdev_detzone_ns_zone *zone;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	struct detzone_io_channel_cong *zone_cong;
	uint64_t zone_idx, sgrp_idx;
	uint64_t phy_offset_blks;
	uint64_t blks_to_submit;
	int rc = 0;

	assert(io_ctx->status == SPDK_BDEV_IO_STATUS_PENDING);
	// SPDK_DEBUGLOG(vbdev_detzone, "SUBMIT: type(READ(%d)) lba(0x%lx) num(0x%lx) phy_offset(0x%lx)\n",
	// 								io_ctx->type, bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks,
	// 								_vbdev_detzone_ns_get_phy_offset(detzone_ns, bdev_io->u.bdev.offset_blocks));

	zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, io_ctx->u.io.next_offset_blocks);
	zone = &detzone_ns->internal.zone[zone_idx];

	if (spdk_unlikely(zone->zone_id + (zone->num_zone_alloc * detzone_ns->base_avail_zcap) <= io_ctx->u.io.next_offset_blocks)) {
		assert(io_ctx->u.io.next_offset_blocks + io_ctx->u.io.remain_blocks <= zone->zone_id + detzone_ns->detzone_ns_bdev.zone_size);
		//assert(zone->num_zone_alloc == 0);
		blks_to_submit = (bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len - io_ctx->u.io.iov_offset)
								 >> spdk_u32log2(detzone_ns->detzone_ns_bdev.blocklen);
		if (spdk_unlikely(blks_to_submit > io_ctx->u.io.remain_blocks)) {
			blks_to_submit = io_ctx->u.io.remain_blocks;
		}
		SPDK_DEBUGLOG(vbdev_detzone, "READ UNALLOCATED: offset(0x%lx) len(0x%lx) remain(0x%lx)\n",
										 io_ctx->u.io.next_offset_blocks,
										 blks_to_submit, io_ctx->u.io.remain_blocks);
		//memset(bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_base + io_ctx->u.io.iov_offset,
		//					0x0, bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len - io_ctx->u.io.iov_offset);
		io_ctx->u.io.remain_blocks -= blks_to_submit;
		if (io_ctx->u.io.outstanding_stripe_ios == 0 && io_ctx->u.io.remain_blocks == 0) {
			if (io_ctx->status != SPDK_BDEV_IO_STATUS_FAILED) {
				io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
			}
			_detzone_ns_io_complete(io_ctx);
		} else {
			io_ctx->u.io.next_offset_blocks += blks_to_submit;
			io_ctx->u.io.iov_offset += blks_to_submit * detzone_ns->detzone_ns_bdev.blocklen;
			assert(io_ctx->u.io.iov_offset <= bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len);
			if (io_ctx->u.io.iov_offset == bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len) {
				io_ctx->u.io.iov_offset = 0;
				io_ctx->u.io.iov_idx++;
			}
		}
		if (io_ctx->u.io.remain_blocks > 0) {
			assert(0);
			_detzone_ns_io_read_submit(io_ctx);
		}
		return;
	}

	sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, io_ctx->u.io.next_offset_blocks);
	assert(sgrp_idx < zone->num_zone_alloc);
	stripe_group = &zone->stripe_group[sgrp_idx];
	// for congestion control, we must use the same idx (the base_idx of the group)
	assert(sgrp_idx >= stripe_group->base_start_idx && zone->num_zone_alloc > stripe_group->base_start_idx);
	sgrp_idx = stripe_group->base_start_idx;
	zone_cong = &detzone_ch->zone_cong[zone_idx];

	if (TAILQ_FIRST(&zone_cong->sgrp[sgrp_idx].pending) != io_ctx) {
		TAILQ_INSERT_TAIL(&zone_cong->sgrp[sgrp_idx].pending, io_ctx, link);
		// SPDK_DEBUGLOG(vbdev_detzone, "queueing (%p) (0x%lx)                  : curr(%u) remain_blks(0x%lx)\n",
		// 			&zone_cong->sgrp[sgrp_idx].pending, io_ctx->u.io.next_offset_blocks, sgrp_idx, io_ctx->u.io.remain_blocks);
	}
	if (TAILQ_FIRST(&zone_cong->sgrp[sgrp_idx].pending) != io_ctx) {
		assert(io_ctx->u.io.outstanding_stripe_ios == 0);
		// io_ctx = TAILQ_FIRST(&zone_cong->sgrp[sgrp_idx].pending);
		// SPDK_DEBUGLOG(vbdev_detzone, "found pending (0x%lx)               : curr(%u) remain_blks(0x%lx)\n",
		// 			io_ctx->u.io.next_offset_blocks, sgrp_idx, io_ctx->u.io.remain_blocks);
		return;
	}

#ifdef DETZONE_NO_CC
	zone_cong->sgrp[sgrp_idx].cwnd = 65536;
#else
	if (zone_cong->sgrp[sgrp_idx].outstandings == 0) {
		//zone_cong->sgrp[sgrp_idx].cwnd = stripe_group->width;
	} else if (zone_cong->sgrp[sgrp_idx].cwnd
					<= zone_cong->sgrp[sgrp_idx].outstandings) {
		assert(zone_cong->sgrp[sgrp_idx].outstandings > 0);
		return;
	}
#endif

	// if this thread has exclusive permission to submit IOs to the stripe group,
	// as long as it holds the permission, it is safe to access data structures
	// except the pending queue
	// It is ok to read the first item only if it is the current io_ctx
	assert(io_ctx->u.io.remain_blocks > 0);
	while (io_ctx->u.io.remain_blocks) {
		phy_offset_blks = _vbdev_detzone_ns_get_phy_offset(detzone_ns, io_ctx->u.io.next_offset_blocks);
		assert(phy_offset_blks != UINT64_MAX);
		//blks_to_submit = spdk_min(io_ctx->u.io.remain_blocks,
		//				stripe_group->stripe_blks - (io_ctx->u.io.next_offset_blocks % stripe_group->stripe_blks));
		blks_to_submit = stripe_group->stripe_blks - (io_ctx->u.io.next_offset_blocks & (stripe_group->stripe_blks - 1));
		if (blks_to_submit > io_ctx->u.io.remain_blocks) {
			blks_to_submit = io_ctx->u.io.remain_blocks;
		}
		// We reuse allocated iovs instead of trying to get new one. 
		// It is likely aligned with the io length
		if (spdk_unlikely(blks_to_submit * detzone_ns->detzone_ns_bdev.blocklen >
									bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len - io_ctx->u.io.iov_offset)) {
			blks_to_submit = (bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len - io_ctx->u.io.iov_offset)
												>> spdk_u32log2(detzone_ns->detzone_ns_bdev.blocklen);
			//													/ detzone_ns->detzone_ns_bdev.blocklen;
		}

		// TODO: check if this namespace blocklen is not equal to the base blocklen.
		// detzone_ns_bdev.phys_blocklen != detzone_ns_bdev.blocklen
		// If so, convert it here
		assert(blks_to_submit>0);

		// Only this thread can access the congestion control data

		/*
		if (phy_offset_blks + blks_to_submit > _vbdev_detzone_ns_get_phy_zone_wp(detzone_ns, phy_offset_blks)) {
			SPDK_DEBUGLOG(vbdev_detzone, "READ beyond WP(0x%lx): offset(0x%lx) len(0x%lx) phy_offset(0x%lx)\n",
											_vbdev_detzone_ns_get_phy_zone_wp(detzone_ns, phy_offset_blks),
											io_ctx->u.io.next_offset_blocks,
											blks_to_submit,
											phy_offset_blks);
		}
		*/
		// SPDK_DEBUGLOG(vbdev_detzone, "READ: offset(0x%lx) len(0x%lx) phy_offset(0x%lx)\n",
		// 									io_ctx->u.io.next_offset_blocks,
		// 									blks_to_submit,
		// 									phy_offset_blks);

		rc = spdk_bdev_read_blocks(detzone_ctrlr->base_desc, detzone_ch->base_ch,
						bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_base + io_ctx->u.io.iov_offset,
						phy_offset_blks, blks_to_submit, _detzone_ns_io_read_complete,
						bdev_io);
		if (spdk_likely(rc == 0)) {
			io_ctx->u.io.outstanding_stripe_ios++;
			io_ctx->u.io.next_offset_blocks += blks_to_submit;
			io_ctx->u.io.remain_blocks -= blks_to_submit;

			io_ctx->u.io.iov_offset += blks_to_submit * detzone_ns->detzone_ns_bdev.blocklen;
			assert(io_ctx->u.io.iov_offset <= bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len);
			if (io_ctx->u.io.iov_offset == bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len) {
				io_ctx->u.io.iov_offset = 0;
				io_ctx->u.io.iov_idx++;
			}
			zone_cong->sgrp[sgrp_idx].outstandings += 1;
			if (zone_cong->sgrp[sgrp_idx].cwnd 
					<= zone_cong->sgrp[sgrp_idx].outstandings) {
				break;
			} else if (io_ctx->u.io.next_offset_blocks >= stripe_group->slba + (detzone_ns->base_avail_zcap << spdk_u32log2(stripe_group->width))) {
				break;
			}
		} else {
			assert(0);
			if (rc == -ENOMEM) {
				assert(0);
				// TODO: need another queueing
				SPDK_ERRLOG("No memory, start to queue io for delay.\n");
				//if (vbdev_detzone_queue_io(bdev_io) != 0) {
				//	goto error_out;
				//}
				return;
			} else {
				SPDK_ERRLOG("ERROR on bdev_io submission!\n");
				goto error_out;
			}
		}
	}

out:
	if (io_ctx->u.io.remain_blocks == 0) {
		TAILQ_REMOVE(&zone_cong->sgrp[sgrp_idx].pending, io_ctx, link);
		// SPDK_DEBUGLOG(vbdev_detzone, "read (0x%lx) submit done                  : curr(%u) remain_blks(0x%lx)\n",
		// 					io_ctx->u.io.next_offset_blocks, sgrp_idx, io_ctx->u.io.remain_blocks);
	} else if (io_ctx->u.io.next_offset_blocks
					 >= stripe_group->slba + (detzone_ns->base_avail_zcap << spdk_u32log2(stripe_group->width))) {
		// SPDK_DEBUGLOG(vbdev_detzone, "move read (0x%lx) to the next stripe group: curr(%u) remain_blks(0x%lx)\n",
		// 					io_ctx->u.io.next_offset_blocks, sgrp_idx, io_ctx->u.io.remain_blocks);
		TAILQ_REMOVE(&zone_cong->sgrp[sgrp_idx].pending, io_ctx, link);
		_detzone_ns_io_read_submit(io_ctx);
	} else {
		// SPDK_DEBUGLOG(vbdev_detzone, "read (0x%lx) continue                     : curr(%u) remain_blks(0x%lx) cwnd(%u) outs(%u)\n",
		// 					io_ctx->u.io.next_offset_blocks, sgrp_idx, io_ctx->u.io.remain_blocks,
		// 					zone_cong->sgrp[sgrp_idx].cwnd,
		// 					zone_cong->sgrp[sgrp_idx].outstandings );
	}
	return;

error_out:
	io_ctx->u.io.remain_blocks = 0;
	io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
	// only complete if there is no outstandings
	if (io_ctx->u.io.outstanding_stripe_ios == 0) {
		TAILQ_REMOVE(&zone_cong->sgrp[sgrp_idx].pending, io_ctx, link);
		_detzone_ns_io_complete(io_ctx);
		io_ctx = TAILQ_FIRST(&zone_cong->sgrp[sgrp_idx].pending);
		if (io_ctx != NULL) {
			_detzone_ns_io_read_submit(io_ctx);
		}
	}
}

static void
_detzone_ns_read_get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io, bool success)
{
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns,
					 detzone_ns_bdev);
	if (spdk_unlikely(!success)) {
		goto error;
	}
	_detzone_ns_io_read_submit(io_ctx);

	// if (detzone_ns->primary_thread) {
	// 	io_ctx->ch = detzone_ns->primary_ch;
	// 	spdk_thread_send_msg(detzone_ns->primary_thread, _detzone_ns_io_read_submit, io_ctx);
	// } else {
	// 	_detzone_ns_io_read_submit(io_ctx);
	// }

	return;

error:
	io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
	_detzone_ns_io_complete(io_ctx);
}

static void
_vbdev_detzone_ns_submit_request_read(void *arg)
{
	struct spdk_bdev_io *bdev_io = arg;

	if (spdk_get_thread() != spdk_bdev_io_get_thread(bdev_io)) {
		spdk_thread_send_msg(spdk_bdev_io_get_thread(bdev_io), _vbdev_detzone_ns_submit_request_read, bdev_io);
		return;
	}
	spdk_bdev_io_get_buf(bdev_io, _detzone_ns_read_get_buf_cb,
			bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
}

static inline void
_vbdev_detzone_ns_zone_wp_forward(struct vbdev_detzone_ns *detzone_ns, struct vbdev_detzone_ns_zone *zone, uint64_t numblocks)
{
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	//SPDK_DEBUGLOG(vbdev_detzone, "forward logi zone_wp : id(%lu) wp(%lu) forward(%lu)\n",
	//				detzone_ns->internal.zone[_vbdev_detzone_ns_get_zone_idx(detzone_ns, offset_blocks)].zone_id,
	//				detzone_ns->internal.zone[_vbdev_detzone_ns_get_zone_idx(detzone_ns, offset_blocks)].write_pointer,
	//				numblocks);
	assert(zone->write_pending_blks >= numblocks);
	zone->write_pointer += numblocks;
	zone->write_pending_blks -= numblocks;
	if (zone->write_pointer == zone->zone_id + zone->num_zone_alloc * detzone_ns->base_avail_zcap) {
		// stripe group FULL
		//SPDK_DEBUGLOG(vbdev_detzone, "Stripe group FULL (0x%lx): width(%u) base_width(%u) used_spares(%u)\n",
		//							zone->stripe_group[zone->num_zone_alloc - 1].slba,
		//							zone->stripe_group[zone->num_zone_alloc - 1].width,
		//							detzone_ctrlr->base_width,
		//							detzone_ns->internal.used_spares);
		detzone_ns->internal.used_spares -= zone->stripe_group[zone->num_zone_alloc - 1].width - detzone_ctrlr->base_width;
	}
	if (zone->write_pointer == zone->zone_id + zone->capacity) {
		assert(zone->write_pending_blks == 0);
		zone->state = SPDK_BDEV_ZONE_STATE_FULL;
		detzone_ns->num_open_zones--;
		detzone_ns->num_active_zones--;
		TAILQ_REMOVE(&detzone_ns->internal.active_zones, zone, active_link);
		SPDK_DEBUGLOG(vbdev_detzone, "ZONE_FULL (0x%lx): release open(%u) active(%u)\n",
								 zone->zone_id,
								 detzone_ns->num_open_zones,
								 detzone_ns->num_active_zones);
		//_verify_active_zone_list(detzone_ns);
	}
}

static inline void
_detzone_ns_io_write_abort_pending(struct vbdev_detzone_ns_zone *zone)
{
	struct detzone_bdev_io *tmp_ctx, *io_ctx;
	// All pending IOs to fail
	TAILQ_FOREACH_SAFE(io_ctx, &zone->wr_pending, link, tmp_ctx) {
		io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
		TAILQ_REMOVE(&zone->wr_pending, io_ctx, link);
		_detzone_ns_io_complete(io_ctx);
	}
	zone->write_pending_blks = 0;
}
/* Completion callback for write IO that were issued from this bdev.
 * We'll check the stripe IO and complete original bdev_ios with the appropriate status
 * and then free the one that this module issued.
 */
static void
_detzone_ns_io_write_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct vbdev_detzone_ns_zone *zone = cb_arg;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	struct detzone_bdev_io *tmp_ctx, *io_ctx;
	struct spdk_bdev_io *orig_io;
	struct vbdev_detzone_ns *detzone_ns;
	struct vbdev_detzone *detzone_ctrlr;
	struct detzone_io_channel *detzone_ch;
	struct vbdev_detzone_ns_shrink_zone_ctx *shrink_ctx;
	uint64_t io_latency_tsc;

	// Get the namespace info using io_ctx at the head
	if (TAILQ_EMPTY(&zone->wr_wait_for_cpl)) {
		io_ctx = TAILQ_FIRST(&zone->wr_pending);
	} else {
		io_ctx = TAILQ_FIRST(&zone->wr_wait_for_cpl);
	}
	assert(io_ctx);
	orig_io = spdk_bdev_io_from_ctx(io_ctx);
	detzone_ch = spdk_io_channel_get_ctx(io_ctx->ch);
	detzone_ns = SPDK_CONTAINEROF(orig_io->bdev, struct vbdev_detzone_ns,
					 											detzone_ns_bdev);
	detzone_ctrlr = detzone_ns->ctrl;
	stripe_group = &zone->stripe_group[_vbdev_detzone_ns_get_sgrp_idx(detzone_ns, zone->write_pointer)];

	io_ctx = NULL;
	orig_io = NULL;

	if (!success) {
		// TODO: we will need a recovery mechanism for this case.
		TAILQ_FOREACH(io_ctx, &zone->wr_wait_for_cpl, link) {
			io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
			spdk_bdev_io_get_nvme_status(bdev_io, &io_ctx->nvme_status.cdw0, &io_ctx->nvme_status.sct, &io_ctx->nvme_status.sc);
		}
		assert(0);
	} else {
		uint64_t zone_idx;
		zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, zone->zone_id);
		io_latency_tsc = spdk_get_ticks() - spdk_bdev_io_get_submit_tsc(bdev_io);
#ifdef DETZONE_UBENCH
		pthread_spin_lock(&detzone_ch->lock);
#endif
		detzone_ch->zone_cong[zone_idx].write_blks += bdev_io->u.bdev.num_blocks;
		detzone_ch->zone_cong[zone_idx].write_blks_tsc += io_latency_tsc;
		if (detzone_ch->zone_cong[zone_idx].write_blks == bdev_io->u.bdev.num_blocks) {
			TAILQ_INSERT_TAIL(&detzone_ch->write_zones, &detzone_ch->zone_cong[zone_idx], link);
		}
#ifdef DETZONE_UBENCH
		pthread_spin_unlock(&detzone_ch->lock);
#endif
		// fprintf(stderr, "write latency (0x%lx) (%llu / %llu us) blks(%lu)\n",
		// 										zone->zone_id,
		// 										 io_latency_tsc * SPDK_SEC_TO_USEC / spdk_get_ticks_hz(),
		// 										detzone_ctrlr->blk_latency_thresh_ticks * SPDK_SEC_TO_USEC / spdk_get_ticks_hz(),
		// 										bdev_io->u.bdev.num_blocks);
		// }
		if (1) {}
		else if (io_latency_tsc > detzone_ctrlr->wr_lat_max_thresh) {
		//if (0) {
			zone->tb_cpl_bytes = 0;
			//zone->tb_size = spdk_max(zone->tb_size >> 1, stripe_group->stripe_blks * stripe_group->width);
			zone->tb_size = spdk_max(zone->tb_size * detzone_ctrlr->wr_lat_max_thresh / io_latency_tsc, stripe_group->stripe_blks * stripe_group->width);
			//if (io_latency_tsc >> spdk_u32log2(bdev_io->u.bdev.num_blocks) > detzone_ctrlr->wr_lat_max_thresh << 1) {
			if (1) {
				fprintf(stderr, "write congestion(%llu / %llu us): sgrp_slba(0x%lx) outs(%u) blks(0x%lx) tb_size(0x%lx)\n",
												io_latency_tsc * SPDK_SEC_TO_USEC / spdk_get_ticks_hz(),
												detzone_ctrlr->blk_latency_thresh_ticks * SPDK_SEC_TO_USEC / spdk_get_ticks_hz(),
												stripe_group->slba,
												zone->wr_outstanding_ios,
												bdev_io->u.bdev.num_blocks,
												zone->tb_size);
			}
		} else {
			//zone->tb_cpl_bytes += bdev_io->u.bdev.num_blocks << (spdk_u32log2(detzone_ns->detzone_ns_bdev.blocklen) - 3);
			//zone->tb_size += stripe_group->stripe_blks;
			//zone->tb_size += stripe_group->width;
			//if (zone->tb_cpl_bytes > stripe_group->stripe_blks << spdk_u32log2(detzone_ns->detzone_ns_bdev.blocklen)) {
			if (1) {
				zone->tb_size = spdk_min(zone->tb_size + 1, zone->tb_size_max);
				//zone->tb_size = spdk_min(zone->tb_size + detzone_ctrlr->base_stripe_blks, detzone_ctrlr->base_stripe_blks * stripe_group->width);
				//zone->tb_size = spdk_min(zone->tb_size + stripe_group->stripe_blks, detzone_ctrlr->per_zone_mdts * stripe_group->width);
				zone->tb_cpl_bytes = 0;
			}
		}

		_vbdev_detzone_ns_forward_phy_zone_wp(detzone_ns, bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);
		_vbdev_detzone_ns_zone_wp_forward(detzone_ns, zone, bdev_io->u.bdev.num_blocks);
	}
	spdk_bdev_free_io(bdev_io);
	zone->wr_outstanding_ios -= 1;
	if (zone->wr_outstanding_ios == 0 && zone->wr_zone_in_progress == stripe_group->width) {
	//if (zone->wr_outstanding_ios == 0 && zone->wr_blks_in_flight == zone->wr_blks_to_sumbit) {
		// complete original bdev_ios
		TAILQ_FOREACH_SAFE(io_ctx, &zone->wr_wait_for_cpl, link, tmp_ctx) {
			TAILQ_REMOVE(&zone->wr_wait_for_cpl, io_ctx, link);
			orig_io = spdk_bdev_io_from_ctx(io_ctx);
			if (io_ctx->status != SPDK_BDEV_IO_STATUS_FAILED) {
				io_ctx->status = SPDK_BDEV_IO_STATUS_SUCCESS;
				//_vbdev_detzone_ns_zone_wp_forward(detzone_ns, zone, orig_io->u.bdev.num_blocks - io_ctx->u.io.boundary_blks);
			}
			_detzone_ns_io_complete(io_ctx);
		}
		zone->wr_zone_in_progress = 0;
/* 		zone->wr_blks_in_flight = 0;
		zone->wr_blks_to_sumbit = 0;
 */
		//SPDK_DEBUGLOG(vbdev_detzone, "WRITE: Complete batch: zone_id(0x%lx) wp(0x%lx)\n", zone->zone_id, zone->write_pointer);
#ifdef DETZONE_OVERDRIVE
		if (!TAILQ_EMPTY(&zone->wr_pending)) {
			_detzone_ns_write_prepare(zone);
		}
#else
		shrink_ctx = _vbdev_detzone_ns_get_zone_is_shrink(detzone_ns, zone->write_pointer, false);;
		if (shrink_ctx && !shrink_ctx->is_writable) {
			spdk_thread_send_msg(detzone_ctrlr->thread, vbdev_detzone_ns_shrink_zone_submit, shrink_ctx);
		} else if (!TAILQ_EMPTY(&zone->wr_pending)) {
			_detzone_ns_write_prepare(zone);
		}
#endif
	} else if (zone->wr_zone_in_progress != stripe_group->width) {
		if (_detzone_ns_io_write_submit(spdk_io_channel_from_ctx(detzone_ch), detzone_ns, zone) == 0) {
			TAILQ_REMOVE(&detzone_ns->internal.sched_zones, zone, wr_sched_link);
		}
	}
/* 
	} else if (zone->wr_outstanding_ios == 0 && zone->wr_blks_in_flight != zone->wr_blks_to_sumbit) {
		if (_detzone_ns_io_write_submit(spdk_io_channel_from_ctx(detzone_ch), detzone_ns, zone) == 0) {
			TAILQ_REMOVE(&detzone_ns->internal.sched_zones, zone, wr_sched_link);
		}
	}
 */

	return;
}

static int
_detzone_ns_io_write_submit(struct spdk_io_channel *ch, struct vbdev_detzone_ns *detzone_ns, struct vbdev_detzone_ns_zone *zone)
{
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	struct detzone_bdev_io *io_ctx, *tmp_ctx;
	uint64_t basezone_idx;
	uint64_t avail_tokens_per_stripe, blks_to_submit;
	uint32_t iovs_to_submit;
	uint32_t i, j;
	int rc = 0;
	uint64_t now;

	assert(spdk_get_thread() == detzone_ns->primary_thread);
	stripe_group = &zone->stripe_group[_vbdev_detzone_ns_get_sgrp_idx(detzone_ns, zone->write_pointer)];
/* 
	assert(zone->wr_blks_in_flight < zone->wr_blks_to_sumbit);
	if (zone->wr_outstanding_ios == 0) {
		// init the write batch
		uint64_t generated_tokens;
		zone->wr_outstanding_ios = stripe_group->width;
		now = spdk_get_ticks();
		generated_tokens = (now - zone->tb_last_update_tsc) * stripe_group->width
									 / detzone_ctrlr->blk_latency_thresh_ticks;
		if (generated_tokens > 0) {
			zone->tb_tokens = spdk_min(zone->tb_tokens + generated_tokens,
										zone->tb_size_max);
			zone->tb_last_update_tsc = now;
		}
	} else {
		return -EAGAIN;
	}
 */
	if (zone->wr_outstanding_ios == 0 && zone->wr_zone_in_progress == 0) {
		uint64_t generated_tokens;
		zone->wr_outstanding_ios = stripe_group->width;
		now = spdk_get_ticks();
		generated_tokens = (now - zone->tb_last_update_tsc) * stripe_group->width
									 / detzone_ctrlr->blk_latency_thresh_ticks;
		if (generated_tokens > 0) {
			zone->tb_tokens = spdk_min(zone->tb_tokens + generated_tokens,
										zone->tb_size_max);
			zone->tb_last_update_tsc = now;
		}
	}
#ifdef DETZONE_NO_ADMIT_CTRL
	zone->tb_tokens = zone->tb_size_max;
#endif
		// fprintf(stderr, "prev ios still progressing: zone(0x%lx) outs(%u) remains(0x%lx)\n",
		// 				 zone->zone_id, zone->wr_outstanding_ios, zone->wr_blks_to_sumbit);


/* 	avail_tokens_per_stripe = spdk_min(zone->tb_tokens, zone->tb_size) >> spdk_u32log2(stripe_group->width);
	if (avail_tokens_per_stripe < stripe_group->stripe_blks) {
		zone->wr_outstanding_ios = 0;
		//fprintf(stderr, "not enough tokens: zone(0x%lx) avail(%u) stipre_blks(%u)\n", zone->zone_id, avail_tokens_per_stripe, stripe_group->stripe_blks);
		return -EAGAIN;
	}
 */
	if (zone->tb_tokens < zone->base_zone[stripe_group->base_start_idx + zone->wr_zone_in_progress].iovs_blks) {
		//zone->wr_outstanding_ios = 0;
		//fprintf(stderr, "not enough tokens: zone(0x%lx) avail(%u) stipre_blks(%u)\n", zone->zone_id, avail_tokens_per_stripe, stripe_group->stripe_blks);
		//fprintf(stderr, "not enough tokens: zone(0x%lx) toks(%lu) iolen(%lu)\n", zone->zone_id, zone->tb_tokens, zone->base_zone[stripe_group->base_start_idx + zone->wr_zone_in_progress].iovs_blks);
		return -EAGAIN;
	}

/* 	for (i = zone->wr_blks_in_flight; i < zone->wr_blks_to_sumbit; i++) {
		blks_to_submit = 0;
		iovs_to_submit = 0;
		if (zone->base_zone[basezone_idx].iovs_blks == 0) {
			zone->wr_outstanding_ios -= 1;
			continue;
		}
		for (j = zone->base_zone[basezone_idx].iov_offset; j < zone->base_zone[basezone_idx].iov_cnt; j++) {
			iov_blks = zone->base_zone[basezone_idx].iovs[j].iov_len >> spdk_u32log2(detzone_ns->detzone_ns_bdev.blocklen);
			if (blks_to_submit + iov_blks > avail_tokens_per_stripe) {
				break;
			}
			blks_to_submit += iov_blks;
			iovs_to_submit += 1;
		}
		// if (i==0) fprintf(stderr, "avail tokens: avail(%u) stipre_blks(%u) submit_blks(%u/%u)\n",
		// 						 avail_tokens_per_stripe, stripe_group->stripe_blks, blks_to_submit, zone->base_zone[basezone_idx].iovs_blks);
 		if (blks_to_submit == 0) {
			zone->wr_outstanding_ios -= 1;
 */
	for (i = zone->wr_zone_in_progress; i < stripe_group->width; i++) {
		basezone_idx = stripe_group->base_start_idx + i;
		assert(basezone_idx < DETZONE_MAX_STRIPE_WIDTH);
		if (zone->base_zone[basezone_idx].iovs_blks == 0) {
			zone->wr_outstanding_ios -= 1;
		} else if (rc) {
			// previous I/O has failed to submit. we fail remainings too and don't submit them
			zone->wr_outstanding_ios -= 1;
		} else if (zone->tb_tokens < zone->base_zone[basezone_idx].iovs_blks) {
			// SPDK_DEBUGLOG(vbdev_detzone, "WRITE_SUBMIT: Not enough tokens: wp(0x%lx) base_idx(%u) (%lu < %lu)\n",
			// 									zone->write_pointer, 
			// 									basezone_idx,
			// 									zone->tb_tokens, zone->base_zone[basezone_idx].iovs_blks);
			break;
		} else {
#ifdef DETZONE_UBENCH
			rc = spdk_bdev_writev_blocks(detzone_ctrlr->base_desc, detzone_ns->primary_ch,
#else
			rc = spdk_bdev_writev_blocks(detzone_ctrlr->base_desc, detzone_ch->base_ch,
#endif
/* 							&zone->base_zone[basezone_idx].iovs[zone->base_zone[basezone_idx].iov_offset],
							iovs_to_submit,
							_vbdev_detzone_ns_get_phy_zone_wp(detzone_ns, zone->base_zone[basezone_idx].zone_id),
							blks_to_submit,
							_detzone_ns_io_write_complete, zone);
 */
							zone->base_zone[basezone_idx].iovs,
							zone->base_zone[basezone_idx].iov_cnt,
							_vbdev_detzone_ns_get_phy_zone_wp(detzone_ns, zone->base_zone[basezone_idx].zone_id),
							zone->base_zone[basezone_idx].iovs_blks,
							_detzone_ns_io_write_complete, zone);
			// SPDK_DEBUGLOG(vbdev_detzone, "WRITE: logi_zone_id(0x%lx) wp(0x%lx) basezone_idx(%lu) phy_offset(0x%lx) len(0x%lx)\n",
			// 				zone->zone_id,
			// 				zone->write_pointer,
			// 				basezone_idx,
			// 				_vbdev_detzone_ns_get_phy_zone_wp(detzone_ns, zone->base_zone[basezone_idx].zone_id),
			// 				blks_to_submit);
			assert(rc == 0);
			zone->tb_tokens -= zone->base_zone[basezone_idx].iovs_blks;
/* 			zone->tb_tokens -= blks_to_submit;
			zone->base_zone[basezone_idx].iov_offset += iovs_to_submit;
			zone->base_zone[basezone_idx].iov_blks_in_flight += blks_to_submit;
 */		}

/* 		if (zone->base_zone[basezone_idx].iov_blks_in_flight == zone->base_zone[basezone_idx].iovs_blks) {
			zone->base_zone[basezone_idx].iovs_blks = 0;
			zone->base_zone[basezone_idx].iov_cnt = 0;
			zone->base_zone[basezone_idx].iov_offset = 0;
			zone->base_zone[basezone_idx].iov_blks_in_flight = 0;
		}
		zone->wr_blks_in_flight += blks_to_submit;
 */		
		zone->base_zone[basezone_idx].iovs_blks = 0;
		zone->base_zone[basezone_idx].iov_cnt = 0;
		zone->wr_zone_in_progress += 1;
	}

	if (spdk_unlikely(rc != 0)) {
		// Mark all original IOs to fail
		TAILQ_FOREACH(io_ctx, &zone->wr_wait_for_cpl, link) {
			io_ctx->status = SPDK_BDEV_IO_STATUS_FAILED;
		}
		// If no I/O has been submitted, we have to complete here.
		if (zone->wr_outstanding_ios == 0) {
			TAILQ_FOREACH_SAFE(io_ctx, &zone->wr_wait_for_cpl, link, tmp_ctx) {
				TAILQ_REMOVE(&zone->wr_wait_for_cpl, io_ctx, link);
				_detzone_ns_io_complete(io_ctx);
			}
			zone->wr_zone_in_progress = 0;
			//zone->wr_blks_in_flight = 0;
		}
	} else if (zone->wr_zone_in_progress != stripe_group->width) {
	//} else if (zone->wr_blks_in_flight != zone->wr_blks_to_sumbit) {
		rc = -EAGAIN;
	}
	return rc;
}

static inline int
_vbdev_detzone_ns_append_zone_iov(struct vbdev_detzone_ns *detzone_ns, struct vbdev_detzone_ns_zone *zone,
										 struct vbdev_detzone_ns_stripe_group *stripe_group, uint64_t slba,
										 void *buf, uint64_t *blockcnt)
{
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	uint64_t basezone_idx;
	int iov_idx;

	//assert(zone->wr_blks_in_flight == 0);
	assert(zone->wr_zone_in_progress == 0);
	assert(stripe_group->slba != UINT64_MAX);
	basezone_idx = stripe_group->base_start_idx
						+ (((slba - stripe_group->slba) / stripe_group->stripe_blks)
							% stripe_group->width);
	
	(*blockcnt) = spdk_min((*blockcnt), detzone_ctrlr->per_zone_mdts - zone->base_zone[basezone_idx].iovs_blks);
	if ((*blockcnt) == 0
		 || zone->base_zone[basezone_idx].iov_cnt == DETZONE_WRITEV_MAX_IOVS) {
		// || zone->base_zone[basezone_idx].iov_cnt == 1) {
		return -EAGAIN;
	}
	iov_idx = zone->base_zone[basezone_idx].iov_cnt;
	zone->base_zone[basezone_idx].iovs[iov_idx].iov_base = buf;
	zone->base_zone[basezone_idx].iovs[iov_idx].iov_len = (*blockcnt) * detzone_ns->detzone_ns_bdev.blocklen;
	zone->base_zone[basezone_idx].iovs_blks += (*blockcnt);
	zone->base_zone[basezone_idx].iov_cnt += 1;
	//zone->wr_blks_to_sumbit += (*blockcnt);
	return 0;
}

static int
_detzone_ns_io_write_split(struct vbdev_detzone_ns *detzone_ns, struct spdk_bdev_io *bdev_io)
{
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	struct vbdev_detzone_ns_zone *zone;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	uint64_t blks_to_split;
	uint64_t zone_idx, sgrp_idx;
	int rc = 0;

	while (io_ctx->u.io.remain_blocks) {
		// TODO: use '&' operator rather than '%'
		zone_idx = _vbdev_detzone_ns_get_zone_idx(detzone_ns, io_ctx->u.io.next_offset_blocks);
		sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, io_ctx->u.io.next_offset_blocks);
		zone = &detzone_ns->internal.zone[zone_idx];
		stripe_group = &zone->stripe_group[sgrp_idx];

		if (stripe_group->slba == UINT64_MAX) {
			rc = -ENOSPC;
			break;
		}
		blks_to_split = spdk_min(io_ctx->u.io.remain_blocks,
						stripe_group->stripe_blks - (io_ctx->u.io.next_offset_blocks & (stripe_group->stripe_blks - 1)));	

		if (spdk_unlikely(blks_to_split * detzone_ns->detzone_ns_bdev.blocklen >
									bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len - io_ctx->u.io.iov_offset)) {
			blks_to_split = (bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_len - io_ctx->u.io.iov_offset) /
																	detzone_ns->detzone_ns_bdev.blocklen;
		}
		assert(blks_to_split>0);

		rc = _vbdev_detzone_ns_append_zone_iov(detzone_ns, zone, stripe_group, io_ctx->u.io.next_offset_blocks,
									 bdev_io->u.bdev.iovs[io_ctx->u.io.iov_idx].iov_base + io_ctx->u.io.iov_offset,
									 &blks_to_split);

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
_detzone_ns_write_cb(struct spdk_bdev_io *bdev_io, int sct, int sc, void *cb_arg)
{
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns,
					 detzone_ns_bdev);
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	struct vbdev_detzone_ns_zone *zone = cb_arg;

	assert(spdk_get_thread() == detzone_ns->primary_thread);
	if (spdk_unlikely(sct || sc)) {
		io_ctx->nvme_status.sct = sct;
		io_ctx->nvme_status.sc = sc;
		_detzone_ns_io_write_abort_pending(zone);
		return;
	}
	_detzone_ns_write_prepare(zone);
}

static void
_detzone_ns_write_prepare(void *arg)
{
	struct vbdev_detzone_ns_zone *zone = arg;
	struct vbdev_detzone_ns *detzone_ns;
	struct spdk_bdev_io *bdev_io;
	struct detzone_bdev_io *io_ctx;
	struct spdk_io_channel *ch;
	uint64_t alloc_capacity;
	int rc = 0;

	io_ctx = TAILQ_FIRST(&zone->wr_pending);
	if (!io_ctx) {
		return;
	}
	ch = io_ctx->ch;
	bdev_io = spdk_bdev_io_from_ctx(io_ctx);
	detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns, detzone_ns_bdev);
	zone = &detzone_ns->internal.zone[_vbdev_detzone_ns_get_zone_idx(detzone_ns, io_ctx->u.io.next_offset_blocks)];
	alloc_capacity = zone->num_zone_alloc * detzone_ns->base_avail_zcap;
	//assert(zone->wr_blks_in_flight == 0);
	//assert(zone->wr_blks_to_sumbit == 0);
	assert(zone->wr_zone_in_progress == 0);
	assert(TAILQ_EMPTY(&zone->wr_wait_for_cpl));
	assert(spdk_get_thread() == detzone_ns->primary_thread);
	//SPDK_DEBUGLOG(vbdev_detzone, "WRITE_PREPARE: zone_id(0x%lx) offset(0x%lx)\n", zone->zone_id, io_ctx->u.io.next_offset_blocks);
	switch (zone->state) {
	case SPDK_BDEV_ZONE_STATE_CLOSED:
	case SPDK_BDEV_ZONE_STATE_EMPTY:
		rc = _detzone_ns_zone_management(detzone_ns, bdev_io, zone->write_pointer, false,
									SPDK_BDEV_ZONE_OPEN, _detzone_ns_write_cb, zone);
		goto out;
	case SPDK_BDEV_ZONE_STATE_IMP_OPEN:
	case SPDK_BDEV_ZONE_STATE_EXP_OPEN:
		if (io_ctx->u.io.next_offset_blocks - zone->zone_id < alloc_capacity) {
			break;
		} else if (io_ctx->u.io.next_offset_blocks - zone->zone_id == alloc_capacity) {
			//SPDK_DEBUGLOG(vbdev_detzone, "WRITE_PREPARE: IMP_OPEN new stripe group (%lu) at (0x%lx)\n",
			//									_vbdev_detzone_ns_get_sgrp_idx(detzone_ns, io_ctx->u.io.next_offset_blocks),
			//									io_ctx->u.io.next_offset_blocks);
			rc = _detzone_ns_zone_management(detzone_ns, bdev_io, io_ctx->u.io.next_offset_blocks, false,
										SPDK_BDEV_ZONE_OPEN, _detzone_ns_write_cb, zone);
		} else {
			SPDK_ERRLOG("Write offset out-of-bound: offset(0x%lx) curr_bound(0x%lx)\n", io_ctx->u.io.next_offset_blocks,
														alloc_capacity);
			io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_BOUNDARY_ERROR;
			rc = -EINVAL;
		}
		goto out;
	default:
		assert(0);
		return;
	}

	while (io_ctx != NULL) {
		bdev_io = spdk_bdev_io_from_ctx(io_ctx);
		// SPDK_DEBUGLOG(vbdev_detzone, "SUBMIT: type(WRITE(%d)) lba(0x%lx) num(0x%lx) curr_off(0x%lx)\n",
		// 						io_ctx->type, bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, io_ctx->u.io.next_offset_blocks);

		rc = _detzone_ns_io_write_split(detzone_ns, bdev_io);
		// Some portion of this IO may not be submitted in the current batch.
		// Thus, we move this IO to the write for cpl queue only if whole blocks have submitted.
		if (rc) {
			break;
		}
		TAILQ_REMOVE(&zone->wr_pending, io_ctx, link);
		TAILQ_INSERT_TAIL(&zone->wr_wait_for_cpl, io_ctx, link);
		io_ctx = TAILQ_FIRST(&zone->wr_pending);
	}

	// try to submit write I/O for the batch
	rc = _detzone_ns_io_write_submit(ch, detzone_ns, zone);

out:
	if (rc == -EAGAIN) {
		// add this batch to the scheduler
		//SPDK_DEBUGLOG(vbdev_detzone, "WRITE_SCHEDULER: add zone(0x%lx) for scheduling\n", zone->zone_id);
		TAILQ_INSERT_TAIL(&detzone_ns->internal.sched_zones, zone, wr_sched_link);
	} else if (rc != 0) {
		SPDK_ERRLOG("Error in I/O preparing: %s\n", strerror(-rc));
		_detzone_ns_io_write_abort_pending(zone);
	}
	return;
}

static int
vbdev_detzone_ns_write_sched(void *arg)
{
#ifdef DETZONE_UBENCH
	struct vbdev_detzone_ns *detzone_ns = arg;
	struct spdk_io_channel *ch = spdk_io_channel_from_ctx(detzone_ns->primary_ch);
#else
	struct detzone_io_channel *detzone_ch = arg;
	struct spdk_io_channel *ch = spdk_io_channel_from_ctx(detzone_ch);
	struct vbdev_detzone_ns *detzone_ns = spdk_io_channel_get_io_device(ch);
#endif
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct vbdev_detzone_ns_zone *zone, *tmp_zone;
	struct vbdev_detzone_ns_stripe_group *stripe_group;
	uint64_t now = spdk_get_ticks();
	uint64_t generated_tokens;
	uint64_t sgrp_idx;
	uint32_t resumed_zones = 0;
	uint64_t total_elapsed_tsc = 0;
	uint64_t total_gen_tokens = 0;
	uint64_t first_zone_id = 0;
	int rc = 0;

	assert(spdk_get_thread() == detzone_ns->primary_thread);
	if (TAILQ_EMPTY(&detzone_ns->internal.sched_zones)) {
		return SPDK_POLLER_IDLE;
	}

	TAILQ_FOREACH_SAFE(zone, &detzone_ns->internal.sched_zones, wr_sched_link, tmp_zone) {
		// refill tokens
		sgrp_idx = _vbdev_detzone_ns_get_sgrp_idx(detzone_ns, zone->write_pointer);
		stripe_group = &zone->stripe_group[sgrp_idx];
		generated_tokens = (now - zone->tb_last_update_tsc) * stripe_group->width
									 / detzone_ctrlr->blk_latency_thresh_ticks;
		if (generated_tokens < stripe_group->stripe_blks) {
			continue;
		}
		total_elapsed_tsc += now - zone->tb_last_update_tsc;
		total_gen_tokens += generated_tokens;
		resumed_zones += 1;
		if (first_zone_id == 0) {
			first_zone_id = zone->zone_id;
		}
		zone->tb_tokens += generated_tokens;
		zone->tb_tokens = spdk_min(zone->tb_tokens + generated_tokens,
									 zone->tb_size_max);
		//zone->tb_tokens = spdk_min(zone->tb_tokens, detzone_ns->zone_stripe_tb_size * stripe_group->width);
		//zone->tb_tokens = spdk_min(zone->tb_tokens, 2 * stripe_group->stripe_blks * stripe_group->width);
		zone->tb_last_update_tsc = now;
		//fprintf(stderr, "tb refilled: zone(0x%lx) 0x%lx blks\n", zone->zone_id, zone->tb_tokens);
		rc = _detzone_ns_io_write_submit(ch, detzone_ns, zone);
		if (rc == -EAGAIN) {
			continue;
		}
		TAILQ_REMOVE(&detzone_ns->internal.sched_zones, zone, wr_sched_link);
		if (rc != 0) {
			_detzone_ns_io_write_abort_pending(zone);
		}
	}
	// if (resumed_zones) {
	// 	fprintf(stderr, "zone resumed: zones(%u) first_zone_id(0x%lx) avg_gen_tokens(%lu) avg_elapsed(%llu us)\n",
	// 							resumed_zones, first_zone_id, total_gen_tokens / resumed_zones, (total_elapsed_tsc / resumed_zones) * SPDK_SEC_TO_USEC / spdk_get_ticks_hz());
	// }
	return SPDK_POLLER_BUSY;
}

static inline uint64_t
_vbdev_detzone_ns_get_zone_append_pointer(struct vbdev_detzone_ns_zone *zone)
{
	/*
	struct spdk_bdev_io *bdev_io;
	struct detzone_bdev_io *io_ctx;
	uint64_t pending_write_blks = 0;
	TAILQ_FOREACH(io_ctx, &zone->wr_wait_for_cpl, link) {
		bdev_io = spdk_bdev_io_from_ctx(io_ctx);
		pending_write_blks += bdev_io->u.bdev.num_blocks
							 - (io_ctx->u.io.next_offset_blocks
								 - bdev_io->u.bdev.offset_blocks);
	}
	TAILQ_FOREACH(io_ctx, &zone->wr_pending, link) {
		bdev_io = spdk_bdev_io_from_ctx(io_ctx);
		pending_write_blks += bdev_io->u.bdev.num_blocks;
		pending_write_blks += bdev_io->u.bdev.num_blocks
							 - (io_ctx->u.io.next_offset_blocks
								 - bdev_io->u.bdev.offset_blocks);
	}
	return zone->write_pointer + pending_write_blks;
	*/
	return zone->write_pointer + zone->write_pending_blks;
}

static void
_vbdev_detzone_ns_submit_request_write(void *arg)
{
	struct spdk_bdev_io *bdev_io = arg;
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	struct vbdev_detzone_ns *detzone_ns = spdk_io_channel_get_io_device(spdk_bdev_io_get_io_channel(bdev_io));
	struct vbdev_detzone_ns_zone *zone;
	struct vbdev_detzone_ns_shrink_zone_ctx *shrink_ctx;

#ifndef DETZONE_UBENCH
	io_ctx->ch = detzone_ns->primary_ch;
#endif
	io_ctx->u.io.iov_offset = 0;
	io_ctx->u.io.iov_idx = 0;
	io_ctx->u.io.outstanding_stripe_ios = 0;
	io_ctx->u.io.outstanding_stripe_ios = 0;

	zone = &detzone_ns->internal.zone[_vbdev_detzone_ns_get_zone_idx(detzone_ns, bdev_io->u.bdev.offset_blocks)];

	switch (zone->state) {
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
	default:
		break;
	}

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_ZONE_APPEND:
		io_ctx->type = DETZONE_IO_APPEND;
		io_ctx->u.io.remain_blocks = bdev_io->u.bdev.num_blocks;
		if (spdk_unlikely(bdev_io->u.bdev.offset_blocks % detzone_ns->detzone_ns_bdev.zone_size)) {
			io_ctx->nvme_status.sct = SPDK_NVME_SCT_GENERIC;
			io_ctx->nvme_status.sc = SPDK_NVME_SC_INVALID_FIELD;
			goto error_complete;
		} else {
			io_ctx->u.io.next_offset_blocks = _vbdev_detzone_ns_get_zone_append_pointer(zone);
			bdev_io->u.bdev.offset_blocks = io_ctx->u.io.next_offset_blocks;
		}
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		io_ctx->type = DETZONE_IO_WRITE;
		io_ctx->u.io.remain_blocks = bdev_io->u.bdev.num_blocks;
		io_ctx->u.io.next_offset_blocks = bdev_io->u.bdev.offset_blocks;
		if (bdev_io->u.bdev.offset_blocks != zone->write_pointer + zone->write_pending_blks) {
			SPDK_ERRLOG("Invalid WP given: request_wp(0x%lx) curr_wp(0x%lx) zs(%u)\n",
									bdev_io->u.bdev.offset_blocks,
									zone->write_pointer,
									zone->state);
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

	if (io_ctx->u.io.next_offset_blocks + bdev_io->u.bdev.num_blocks >
					zone->zone_id + zone->capacity) {
		io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
		io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_BOUNDARY_ERROR;
		goto error_complete;
	}

	// insert IO to the pending queue
	TAILQ_INSERT_TAIL(&zone->wr_pending, io_ctx, link);
	zone->write_pending_blks += bdev_io->u.bdev.num_blocks;
	if (TAILQ_FIRST(&zone->wr_pending) != io_ctx || !TAILQ_EMPTY(&zone->wr_wait_for_cpl)) {
		// We have outstanding IOs. This IO will be handled by the scheduler
		return;
	}
#ifndef DETZONE_OVERDRIVE
	shrink_ctx = _vbdev_detzone_ns_get_zone_is_shrink(detzone_ns, zone->write_pointer, false);
	if (shrink_ctx && !shrink_ctx->is_writable) {
		// We are shrinking this zone. Write IOs will be resumed once it finish.
		return;
	}
#endif
	_detzone_ns_write_prepare(zone);

	/*
	switch (zone->state) {
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
											 zone->zone_id,
											 false,
											 SPDK_BDEV_ZONE_OPEN, _detzone_ns_write_cb, io_ctx->ch);
		break;
	case SPDK_BDEV_ZONE_STATE_EMPTY:
		rc = _detzone_ns_zone_management(detzone_ns, bdev_io, bdev_io->u.bdev.offset_blocks, false,
										SPDK_BDEV_ZONE_OPEN, _detzone_ns_write_cb, io_ctx->ch);
		break;
	default:
		if (bdev_io->u.bdev.offset_blocks + bdev_io->u.bdev.num_blocks
				<= zone->num_zone_alloc * detzone_ns->base_avail_zcap) {
			_detzone_ns_write_cb(bdev_io, 0, 0, io_ctx->ch);
		} else if (bdev_io->u.bdev.offset_blocks + bdev_io->u.bdev.num_blocks
					> zone->zone_id + zone->capacity) {
			io_ctx->nvme_status.sct = SPDK_NVME_SCT_COMMAND_SPECIFIC;
			io_ctx->nvme_status.sc = SPDK_NVME_SC_ZONE_BOUNDARY_ERROR;
			goto error_complete;
		} else if (bdev_io->u.bdev.offset_blocks
						 < zone->num_zone_alloc * detzone_ns->base_avail_zcap) {
			// I/O crosses the boudary of the stripe group
			// split I/O into two chunks at the boundary
			// submit the chunk of previous group, the other chunk wait for the completion
		} else {
			// I/O begins from the first byte of next stripe group
			if (zone->write_pointer != bdev_io->u.bdev.offset_blocks) {
				// there are outstanding IOs (APPENDs), we have to wait	
			} else {

			}
			rc = _detzone_ns_zone_management(detzone_ns, bdev_io,
												 bdev_io->u.bdev.offset_blocks, false,
												 SPDK_BDEV_ZONE_OPEN, _detzone_ns_write_cb,
												 io_ctx->ch);
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
	*/

	return;

error_complete:
	SPDK_ERRLOG("Cannot submit %s I/O: lba(0x%lx) len(0x%lx) sct,sc(%x,%x)\n",
						bdev_io->type == SPDK_BDEV_IO_TYPE_ZONE_APPEND ? "APPEND" : "WRITE",
						bdev_io->u.bdev.offset_blocks,
						bdev_io->u.bdev.num_blocks,
						io_ctx->nvme_status.sct, io_ctx->nvme_status.sc);
	_detzone_ns_io_complete(io_ctx);
}

static void
vbdev_detzone_ns_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct vbdev_detzone_ns *detzone_ns = SPDK_CONTAINEROF(bdev_io->bdev, struct vbdev_detzone_ns, detzone_ns_bdev);
	struct vbdev_detzone *detzone_ctrlr = detzone_ns->ctrl;
	struct detzone_bdev_io *io_ctx = (struct detzone_bdev_io *)bdev_io->driver_ctx;
	int rc = 0;

	io_ctx->status = SPDK_BDEV_IO_STATUS_PENDING;
	io_ctx->in_shrink = false;
	
	// SPDK_DEBUGLOG(vbdev_detzone, "submit_request: type(%u)\n", bdev_io->type);
	
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
		/* if (_vbdev_detzone_ns_get_zone_state(detzone_ns, bdev_io->u.bdev.offset_blocks) == SPDK_BDEV_ZONE_STATE_CLOSED) {
			SPDK_DEBUGLOG(vbdev_detzone, "READ to CLOSED zone: offset(0x%lx) blks (0x%lx) phy_offset(0x%lx)\n",
												bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks,
												_vbdev_detzone_ns_get_phy_offset(detzone_ns, bdev_io->u.bdev.offset_blocks));
		} */
		io_ctx->type = DETZONE_IO_READ;
		io_ctx->ch = ch;
		io_ctx->u.io.iov_offset = 0;
		io_ctx->u.io.iov_idx = 0;
		io_ctx->u.io.outstanding_stripe_ios = 0;
		io_ctx->u.io.remain_blocks = bdev_io->u.bdev.num_blocks;
		io_ctx->u.io.next_offset_blocks = bdev_io->u.bdev.offset_blocks;
#ifdef DETZONE_OVERDRIVE
		_vbdev_detzone_ns_submit_request_read(bdev_io);
#else
		if (spdk_unlikely(_vbdev_detzone_ns_get_zone_is_shrink(detzone_ns, bdev_io->u.bdev.offset_blocks, false) != NULL)) {
			spdk_thread_send_msg(detzone_ctrlr->thread, _vbdev_detzone_ns_submit_request_read_shrink, bdev_io);
		} else {
			_vbdev_detzone_ns_submit_request_read(bdev_io);
		}
#endif
		break;

	case SPDK_BDEV_IO_TYPE_ZONE_APPEND:
	case SPDK_BDEV_IO_TYPE_WRITE:
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		/* if (_vbdev_detzone_ns_get_zone_state(detzone_ns, bdev_io->u.bdev.offset_blocks) == SPDK_BDEV_ZONE_STATE_CLOSED) {
			SPDK_DEBUGLOG(vbdev_detzone, "WRITE to CLOSED zone: offset(0x%lx) blks (0x%lx)\n",
												bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);
		} */
#ifdef DETZONE_UBENCH
		io_ctx->ch = ch;
#endif
		if (detzone_ns->primary_thread == spdk_get_thread()) {
			_vbdev_detzone_ns_submit_request_write(bdev_io);
		} else {
			spdk_thread_send_msg(detzone_ns->primary_thread, _vbdev_detzone_ns_submit_request_write, bdev_io);
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
	spdk_json_write_named_uint32(w, "num_base_zones",
							 detzone_ns->detzone_ns_bdev.blockcnt / detzone_ns->detzone_ns_bdev.zone_size
							 					* DETZONE_MAX_STRIPE_WIDTH);
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
	uint64_t num_zones = detzone_ns->detzone_ns_bdev.blockcnt / detzone_ns->detzone_ns_bdev.zone_size;

	detzone_ch->base_ch = spdk_bdev_get_io_channel(detzone_ctrlr->base_desc);
	detzone_ch->ch_id = ++detzone_ns->ref;
	detzone_ch->zone_cong = calloc(sizeof(struct detzone_io_channel_cong), num_zones);
	for (uint64_t i = 0; i < num_zones; i++) {
		for (int j = 0; j < DETZONE_MAX_STRIPE_WIDTH; j++) {
			detzone_ch->zone_cong[i].sgrp[j].cwnd = detzone_ctrlr->base_width;
			detzone_ch->zone_cong[i].sgrp[j].cpls = 0;
			detzone_ch->zone_cong[i].sgrp[j].cwnd_update_delay = 0;
			detzone_ch->zone_cong[i].sgrp[j].outstandings = 0;
			TAILQ_INIT(&detzone_ch->zone_cong[i].sgrp[j].pending);
		}
		detzone_ch->zone_cong[i].write_blks = 0;
		detzone_ch->zone_cong[i].write_blks_tsc = 0;
	}
	TAILQ_INIT(&detzone_ch->write_zones);
#ifdef DETZONE_UBENCH
	pthread_spin_init(&detzone_ch->lock, 0);
#endif

	detzone_ch->rd_lat_tsc_thresh = spdk_get_ticks_hz() * 500 / SPDK_SEC_TO_USEC;
	fprintf(stderr, "detzone ns: create io channel %u (is_writable: %d)\n", detzone_ch->ch_id, detzone_ns->primary_thread == NULL);
	//if (detzone_ns->ref == 1) {
#ifndef DETZONE_UBENCH
	if (detzone_ns->primary_thread == NULL) {
		// Set the first io channel as primary channel
		detzone_ns->primary_thread = spdk_get_thread();
		detzone_ns->primary_ch = spdk_io_channel_from_ctx(detzone_ch);
		detzone_ch->write_sched_poller = SPDK_POLLER_REGISTER(vbdev_detzone_ns_write_sched, detzone_ch, 1000);
	}
#endif
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
	if (detzone_ns->primary_ch == spdk_io_channel_from_ctx(detzone_ch)) {
		// TODO: need to move the writer somewhere else if this device has any other active channel
		// by iterating all existing channels...
		//assert(detzone_ns->ref == 0);

		detzone_ns->primary_thread = NULL;
		detzone_ns->primary_ch = NULL;
		spdk_poller_unregister(&detzone_ch->write_sched_poller);
		detzone_ch->write_sched_poller = NULL;
	}
	free(detzone_ch->zone_cong);
	detzone_ch->zone_cong = NULL;
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
	struct vbdev_detzone_ns_zone *zone = detzone_ns->internal.zone;
	uint64_t num_zones = detzone_ns->detzone_ns_bdev.blockcnt / detzone_ns->detzone_ns_bdev.zone_size;

	/* Done with this detzone_ns. */
	free(detzone_ns->detzone_ns_bdev.name);
	spdk_bit_array_free(&detzone_ns->internal.epoch_pu_map);
	spdk_mempool_free(detzone_ns->io_buf_pool);
	spdk_mempool_free(detzone_ns->md_buf_pool);
	free(detzone_ns->internal.zone);
	free(detzone_ns);
}

#ifdef DETZONE_UBENCH
static void
__open_primary_poller(void *arg)
{
	struct vbdev_detzone_ns *detzone_ns = arg;
	detzone_ns->primary_poller = SPDK_POLLER_REGISTER(vbdev_detzone_ns_write_sched, detzone_ns, 500);
}

static void
__close_primary_poller(void *arg)
{
	struct vbdev_detzone_ns *detzone_ns = arg;
	spdk_poller_unregister(arg);
}

#endif

static int
vbdev_detzone_ns_destruct(void *ctx)
{
	struct vbdev_detzone_ns *detzone_ns = (struct vbdev_detzone_ns *)ctx;
	struct vbdev_detzone    *detzone_ctrlr = (struct vbdev_detzone *)detzone_ns->ctrl;
	/* It is important to follow this exact sequence of steps for destroying
	 * a vbdev...
	 */

	TAILQ_REMOVE(&detzone_ctrlr->ns, detzone_ns, link);

#ifdef DETZONE_UBENCH
	spdk_thread_send_msg(detzone_ns->primary_thread, __close_primary_poller, &detzone_ns->primary_poller);
#endif
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
	struct vbdev_detzone_ns_zone *zone;
	struct vbdev_detzone_zone_info *phy_zone;
	struct spdk_bdev *bdev;
	uint64_t total_lzones, i, zone_idx, base_zone_idx;
	uint32_t j;
	int rc = 0;
	char io_pool_name[SPDK_MAX_MEMPOOL_NAME_LEN], md_pool_name[SPDK_MAX_MEMPOOL_NAME_LEN];

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

			total_lzones = assoc->num_base_zones / DETZONE_MAX_STRIPE_WIDTH;
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

			detzone_ns->detzone_ns_bdev.write_cache = bdev->write_cache;
			detzone_ns->detzone_ns_bdev.optimal_io_boundary = bdev->optimal_io_boundary;

			detzone_ns->base_zsze = detzone_ctrlr->mgmt_bdev.zone_size;
			// Configure namespace specific parameters
			// detzone_ns->zone_stripe_tb_size = detzone_ctrlr->per_zone_mdts;

			// Caculate the number of padding blocks (reserve block + meta block + padding)
			// Padding to align the end of base zone to stripe end
			detzone_ns->padding_blocks = DETZONE_RESERVATION_BLKS + DETZONE_INLINE_META_BLKS +
											 (detzone_ctrlr->zone_info[0].capacity - (DETZONE_RESERVATION_BLKS +
											 DETZONE_INLINE_META_BLKS)) % detzone_ctrlr->base_stripe_blks;
			//TODO: should check base_bdev zone capacity
			detzone_ns->base_avail_zcap = detzone_ctrlr->zone_info[0].capacity - detzone_ns->padding_blocks;
			detzone_ns->zcap = detzone_ns->base_avail_zcap * DETZONE_MAX_STRIPE_WIDTH;
			detzone_ns->detzone_ns_bdev.zone_size = spdk_align64pow2(detzone_ns->zcap);
			//detzone_ns->detzone_ns_bdev.zone_size = detzone_ns->zcap;
			detzone_ns->detzone_ns_bdev.required_alignment = bdev->required_alignment;
			detzone_ns->detzone_ns_bdev.max_zone_append_size = bdev->max_zone_append_size;
#ifdef DETZONE_UBENCH
#ifdef DETZONE_UBENCH_OVERDRIVE
			detzone_ns->detzone_ns_bdev.max_open_zones = DETZONE_NS_MAX_OPEN_ZONE;
			detzone_ns->detzone_ns_bdev.max_active_zones = DETZONE_NS_MAX_OPEN_ZONE;
#else
			detzone_ns->detzone_ns_bdev.max_open_zones = 128;
			detzone_ns->detzone_ns_bdev.max_active_zones = 128;
#endif
			detzone_ns->primary_thread = detzone_ctrlr->thread;
			detzone_ns->primary_ch = detzone_ctrlr->mgmt_ch;
			spdk_thread_send_msg(detzone_ns->primary_thread, __open_primary_poller, detzone_ns);
#else
			detzone_ns->detzone_ns_bdev.max_open_zones = DETZONE_NS_MAX_OPEN_ZONE; //spdk_max(1, 64/detzone_ns->zone_stripe_width);
			detzone_ns->detzone_ns_bdev.max_active_zones = DETZONE_NS_MAX_ACTIVE_ZONE; //spdk_max(1, 64/detzone_ns->zone_stripe_width);
#endif
			detzone_ns->detzone_ns_bdev.optimal_open_zones = detzone_ns->detzone_ns_bdev.max_active_zones;
		
			detzone_ns->detzone_ns_bdev.blocklen = bdev->blocklen;
			// TODO: support configurable block length (blocklen)
			//detzone_ns->detzone_ns_bdev.phys_blocklen = bdev->blocklen;

			detzone_ns->detzone_ns_bdev.blockcnt = total_lzones * detzone_ns->detzone_ns_bdev.zone_size;

			rc = snprintf(io_pool_name, SPDK_MAX_MEMPOOL_NAME_LEN, "io_pool_%s_%u",
															 detzone_ctrlr->mgmt_bdev.name,
															 detzone_ns->nsid);
			if (rc < 0 || rc >= SPDK_MAX_MEMPOOL_NAME_LEN) {
				SPDK_ERRLOG("cannot create a mempool\n");
				rc = -ENAMETOOLONG;
				goto error_close;
			}
			detzone_ns->io_buf_pool = spdk_mempool_create(io_pool_name,
											detzone_ns->detzone_ns_bdev.max_active_zones,
											detzone_ctrlr->per_zone_mdts * DETZONE_MAX_STRIPE_WIDTH * detzone_ctrlr->mgmt_bdev.blocklen,
											SPDK_MEMPOOL_DEFAULT_CACHE_SIZE,
											SPDK_ENV_SOCKET_ID_ANY);
			if (!detzone_ns->io_buf_pool) {
				SPDK_ERRLOG("cannot create a mempool\n");
				rc = -ENOMEM;
				goto error_close;
			}
			// SPDK_DEBUGLOG(vbdev_detzone, "create mempool: %s (%ld)\n", io_pool_name, spdk_mempool_count(detzone_ns->io_buf_pool));
			rc = snprintf(md_pool_name, SPDK_MAX_MEMPOOL_NAME_LEN, "md_pool_%s_%u",
															 detzone_ctrlr->mgmt_bdev.name,
															 detzone_ns->nsid);
			if (rc < 0 || rc >= SPDK_MAX_MEMPOOL_NAME_LEN) {
				SPDK_ERRLOG("cannot create a mempool\n");
				rc = -ENAMETOOLONG;
				goto error_close;
			}
			detzone_ns->md_buf_pool = spdk_mempool_create(md_pool_name,
											detzone_ns->detzone_ns_bdev.max_active_zones,
											detzone_ns->padding_blocks
												 * detzone_ctrlr->mgmt_bdev.blocklen * DETZONE_MAX_STRIPE_WIDTH,
											SPDK_MEMPOOL_DEFAULT_CACHE_SIZE,
											SPDK_ENV_SOCKET_ID_ANY);
			if (!detzone_ns->md_buf_pool) {
				SPDK_ERRLOG("cannot create a mempool\n");
				rc = -ENOMEM;
				goto error_close;
			}
			// SPDK_DEBUGLOG(vbdev_detzone, "create mempool: %s (%ld)\n", md_pool_name, spdk_mempool_count(detzone_ns->md_buf_pool));

			detzone_ns->internal.avg_open_zones_milli = detzone_ns->detzone_ns_bdev.max_active_zones << DETZONE_SHIFT_MILLI_BIN;
#ifdef DETZONE_UBENCH
#ifdef DETZONE_UBENCH_OVERDRIVE
			detzone_ns->internal.essentials = detzone_ctrlr->base_essentials;
			detzone_ns->internal.spares = detzone_ctrlr->base_spares;
#else
			detzone_ns->internal.essentials = detzone_ctrlr->base_essentials;
			detzone_ns->internal.spares = detzone_ctrlr->base_essentials * (detzone_ns->nsid - 1);
#endif
#else
			detzone_ns->internal.essentials = detzone_ctrlr->base_essentials;
			detzone_ns->internal.spares = detzone_ctrlr->base_spares;
#endif
			if (detzone_ctrlr->avail_spares >= detzone_ns->internal.spares) {
				detzone_ctrlr->avail_spares -= detzone_ns->internal.spares;
				detzone_ns->internal.lent_spares = 0;
			} else {
				detzone_ns->internal.lent_spares = detzone_ns->internal.spares - detzone_ctrlr->avail_spares;
				detzone_ctrlr->avail_spares = 0;
			}
			detzone_ns->internal.used_spares = 0;
			detzone_ns->internal.leased_spares = 0;
			detzone_ns->internal.reclaimed_spares = 0;

			detzone_ns->internal.epoch_num_pu = 0;
			detzone_ns->internal.epoch_pu_map = spdk_bit_array_create(detzone_ctrlr->num_pu);
			if (!detzone_ns->internal.epoch_pu_map) {
				SPDK_ERRLOG("cannot create a PU Map\n");
				rc = -ENOMEM;
				goto error_close;
			}
			TAILQ_INIT(&detzone_ns->internal.sched_zones);
			TAILQ_INIT(&detzone_ns->internal.active_zones);
			TAILQ_INIT(&detzone_ns->internal.rd_pending);

			// it looks dumb... but let just keep it...
			assert(!SPDK_BDEV_ZONE_STATE_EMPTY);
			zone = detzone_ns->internal.zone;
			phy_zone = detzone_ctrlr->zone_info;
			// Init zone info (set non-zero init values)
			for (i=0; i < total_lzones; i++) {
				zone[i].capacity = detzone_ns->zcap;
				zone[i].zone_id = detzone_ns->detzone_ns_bdev.zone_size * i;
				zone[i].write_pointer = zone[i].zone_id;
				zone[i].last_write_pointer[0] = zone[i].zone_id;
				zone[i].last_write_pointer[1] = zone[i].zone_id;
				zone[i].last_write_pointer[2] = zone[i].zone_id;

				TAILQ_INIT(&zone[i].wr_pending);
				TAILQ_INIT(&zone[i].wr_wait_for_cpl);
				for (j=0; j < DETZONE_MAX_STRIPE_WIDTH; j++) {
					zone[i].base_zone[j].zone_id = UINT64_MAX;
					zone[i].stripe_group[j] = empty_stripe_group;
				}
			}
			// Preload existing zone info
			for (i=DETZONE_RESERVED_ZONES; i < detzone_ctrlr->num_zones; i++) {
				if (phy_zone[i].ns_id != detzone_ns->nsid) {
					continue;
				}
				if (_detzone_is_active_state(phy_zone[i].state)) {
					detzone_ns->num_phy_active_zones += 1;
				}
				if (_detzone_is_open_state(phy_zone[i].state)) {
					detzone_ns->num_phy_open_zones += 1;
				}
				zone_idx = phy_zone[i].lzone_id / detzone_ns->detzone_ns_bdev.zone_size;
				zone[zone_idx].base_zone[phy_zone[i].stripe_id].zone_id = phy_zone[i].zone_id;
				zone[zone_idx].stripe_group[phy_zone[i].stripe_id] = phy_zone[i].stripe_group;
				zone[zone_idx].num_zone_alloc += 1;
				// TODO: if any zone has a partially written stripe, we have to recover.
				// we may copy valid data to another physical zones and discard the partial write. 
				if (phy_zone[i].state == SPDK_BDEV_ZONE_STATE_FULL) {
					zone[zone_idx].write_pointer += phy_zone[i].capacity - detzone_ns->padding_blocks;
				} else {
					zone[zone_idx].write_pointer += phy_zone[i].write_pointer - (
													phy_zone[i].zone_id + detzone_ns->padding_blocks);
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
						uint32_t sgrp_idx = 0;
						if (zone[i].num_zone_alloc != zone[i].capacity / detzone_ns->base_avail_zcap) {
							SPDK_ERRLOG("Can't find all physical zones for the logical zone \n");
							rc = -EINVAL;
							goto error_close;								
						}
						for (j=0; j < zone[i].num_zone_alloc; j++) {
							// TODO: we may need to validate these as well
							base_zone_idx = zone[i].base_zone[j].zone_id / detzone_ns->base_zsze;
							if (j == sgrp_idx + zone[i].stripe_group[sgrp_idx].width) {
								sgrp_idx = j;
							}
							if (phy_zone[base_zone_idx].state != SPDK_BDEV_ZONE_STATE_FULL 
									|| zone[i].stripe_group[j].slba != zone[i].zone_id + (sgrp_idx * detzone_ns->base_avail_zcap)
									|| zone[i].stripe_group[j].width != zone[i].stripe_group[sgrp_idx].width
									|| zone[i].stripe_group[j].stripe_blks != zone[i].stripe_group[sgrp_idx].stripe_blks
									|| zone[i].stripe_group[j].base_start_idx != sgrp_idx) {
								SPDK_ERRLOG("Basezone metadata does not match\n");
								rc = -EINVAL;
								goto error_close;
							}
						}
						//zone[i].write_pointer = zone[i].zone_id;
						zone[i].write_pointer = zone[i].zone_id + zone[i].capacity;
						zone[i].state = SPDK_BDEV_ZONE_STATE_FULL;
						break;
					} else {
						uint32_t sgrp_idx = 0;
						uint64_t write_pointer = zone[i].zone_id;
						uint64_t valid_blks, prev_valid_blks = 0, partial_stripe_blks;
						// Validate the zone write pointer
						for (j=0; j < zone[i].num_zone_alloc; j++) {
							bool is_found = false;
							assert(zone[i].base_zone[j].zone_id != UINT64_MAX);
							if (j == sgrp_idx + zone[i].stripe_group[sgrp_idx].width) {
								sgrp_idx = j;
							}

							base_zone_idx = zone[i].base_zone[j].zone_id / detzone_ns->base_zsze;
							valid_blks = phy_zone[base_zone_idx].write_pointer
																- phy_zone[base_zone_idx].zone_id
																- detzone_ns->padding_blocks;
							partial_stripe_blks = valid_blks % phy_zone[base_zone_idx].stripe_group.stripe_blks;
							if (partial_stripe_blks) {
								if (is_found) {
									SPDK_ERRLOG("Invalid phy zone write pointer for the stripe\n");
									rc = -EINVAL;
									goto error_close;
								} else {
									is_found = true;
								}
							}

							SPDK_DEBUGLOG(vbdev_detzone, "#:%u base_idx:%lu sgrp_idx:%u phy_zid:0x%lx phy_wp:0x%lx valid_blk:0x%lx width:%u stripe_blks:%u logi_wp:0x%lx\n",
											j, base_zone_idx, sgrp_idx, phy_zone[base_zone_idx].zone_id, phy_zone[base_zone_idx].write_pointer, valid_blks,
											phy_zone[base_zone_idx].stripe_group.width, phy_zone[base_zone_idx].stripe_group.stripe_blks, write_pointer);
							if (j == sgrp_idx) {
								write_pointer += valid_blks;
								prev_valid_blks = valid_blks;
								continue;
							}

							if ((valid_blks > prev_valid_blks)
								|| (is_found && partial_stripe_blks)
								|| (prev_valid_blks - valid_blks) / phy_zone[base_zone_idx].stripe_group.stripe_blks > 1 ) {
								SPDK_ERRLOG("Invalid phy zone write pointer for the stripe\n");
								assert(0);
								rc = -EINVAL;
								goto error_close;
							}

							if (valid_blks < prev_valid_blks) {
								is_found = true;
							}
							write_pointer += valid_blks;
							prev_valid_blks = valid_blks;
							if (_detzone_is_active_state(phy_zone[base_zone_idx].state)) {
								detzone_ns->internal.used_spares += phy_zone[base_zone_idx].stripe_group.width - detzone_ctrlr->base_width;
							}
						}

						if (zone[i].write_pointer != write_pointer) {						
							SPDK_ERRLOG("Broken stripe! state:%u zone_id:0x%lx zone_wp:0x%lx cap:0x%lx: incorrect wp:0x%lx\n",
												zone[i].state, zone[i].zone_id, zone[i].write_pointer, zone[i].capacity, write_pointer);
							rc = -EINVAL;
							goto error_close;								
						}
						detzone_ns->num_active_zones += 1;
						TAILQ_INSERT_TAIL(&detzone_ns->internal.active_zones, &zone[i], active_link);
					}

					break;

				default:
					// This case is not possible
					assert(0);
					break;
				}
			}

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
	SPDK_ERRLOG("Failed to validate the previous stripe data\n");
	free(detzone_ns->detzone_ns_bdev.name);
	spdk_bit_array_free(&detzone_ns->internal.epoch_pu_map);
	spdk_mempool_free(detzone_ns->io_buf_pool);
	spdk_mempool_free(detzone_ns->md_buf_pool);
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
	struct vbdev_detzone_ns *detzone_ns = spdk_io_channel_iter_get_io_device(i);
	struct vbdev_detzone *detzone_ctrlr = spdk_io_channel_iter_get_ctx(i);
	struct spdk_io_channel *ch = spdk_io_channel_iter_get_channel(i);
	struct detzone_io_channel *detzone_ch = spdk_io_channel_get_ctx(ch);
	struct detzone_io_channel_cong *zone_cong;

#ifdef DETZONE_UBENCH
		pthread_spin_lock(&detzone_ch->lock);
#endif
	while (!TAILQ_EMPTY(&detzone_ch->write_zones)) {
		zone_cong = TAILQ_FIRST(&detzone_ch->write_zones);
		if (zone_cong->write_blks > 0) {
			detzone_ns->internal.total_written_blks += zone_cong->write_blks;
			detzone_ctrlr->internal.active_write_zones += 1;
			detzone_ctrlr->internal.total_avg_write_blk_tsc += zone_cong->write_blks_tsc
																/ zone_cong->write_blks;
		}
		zone_cong->write_blks = 0;
		zone_cong->write_blks_tsc = 0;
		TAILQ_REMOVE(&detzone_ch->write_zones, zone_cong, link);
	}
#ifdef DETZONE_UBENCH
		pthread_spin_unlock(&detzone_ch->lock);
#endif

	spdk_for_each_channel_continue(i, 0);
}

static void
_vbdev_detzone_wr_stat_done(struct spdk_io_channel_iter *i, int status)
{
	struct vbdev_detzone_ns *detzone_ns = spdk_io_channel_iter_get_io_device(i);
	struct vbdev_detzone *detzone_ctrlr = spdk_io_channel_iter_get_ctx(i);
	struct vbdev_detzone_ns *detzone_ns_next;
	uint64_t	avg_write_lat_tsc;
	static uint64_t counter = 0;

	uint64_t written_blks;
	uint32_t written_zones[5] = {0};

	assert(spdk_get_thread() == detzone_ctrlr->thread);

	detzone_ns_next = TAILQ_NEXT(detzone_ns, link);
	//fprintf(stderr, "find next namespace (%p)\n", detzone_ns_next);

	if (detzone_ns_next == NULL) {
		counter += 1;

		if (counter % 1000 == 0) {
			fprintf(stderr, "%lu, ", spdk_get_ticks() * 1000 / spdk_get_ticks_hz());
			TAILQ_FOREACH(detzone_ns, &detzone_ctrlr->ns, link) {
				written_blks = detzone_ns->internal.total_written_blks - detzone_ns->internal.last_written_blks;
				fprintf(stderr, "%u, %u, %u, %u, %u, %lu, ",
													detzone_ns->nsid,
													detzone_ns->num_active_zones,
													detzone_ns->num_open_zones,
													detzone_ns->num_phy_active_zones,
													detzone_ns->num_phy_open_zones,
													written_blks);
/* 				for (uint64_t i=0; i < detzone_ns->detzone_ns_bdev.blockcnt / detzone_ns->detzone_ns_bdev.zone_size; i++) {
					struct vbdev_detzone_ns_zone *zone;
					zone = &detzone_ns->internal.zone[i];
					if (zone->write_pointer != zone->last_write_pointer[0]) {
						written_zones[0] += 1;
						zone->last_write_pointer[0] = zone->write_pointer;
					}

					if (counter % 3000 == 0 && zone->write_pointer != zone->last_write_pointer[1]) {
						written_zones[1] += 1;
						zone->last_write_pointer[1] = zone->write_pointer;
					}

					if (counter % 10000 == 0 && zone->write_pointer != zone->last_write_pointer[2]) {
						written_zones[2] += 1;
						zone->last_write_pointer[2] = zone->write_pointer;
					}

				}
				if (written_blks) {
					fprintf(stderr, "%lu, ns_id, %u, WAF, %0.2f, actives, %u, opens, %u, phy_actives, %u, phy_opens, %u, %lu, ",
														spdk_get_ticks() * 1000 / spdk_get_ticks_hz(),
														detzone_ns->nsid,
														(detzone_ns->internal.total_shrinked_blks + detzone_ns->internal.total_written_blks) / (1.0 * detzone_ns->internal.total_written_blks),
														detzone_ns->num_active_zones,
														detzone_ns->num_open_zones,
														detzone_ns->num_phy_active_zones,
														detzone_ns->num_phy_open_zones,
														written_blks);
					if (written_zones[0]) {
						fprintf(stderr, "%u, ", written_zones[0]);
					} else {
						fprintf(stderr, ", ");
					}
					written_zones[0] = 0;

					if (counter % 3000 == 0) {
						if (written_zones[1]) {
							fprintf(stderr, "%u, ", written_zones[1]);
						} else {
							fprintf(stderr, ", ");
						}
						written_zones[1] = 0;
					} else {
						fprintf(stderr, ", ");
					}

					if (counter % 10000 == 0) {
						if (written_zones[2]) {
							fprintf(stderr, "%u, \n", written_zones[2]);
						} else {
							fprintf(stderr, ", \n");
						}
						written_zones[2] = 0;
					} else {
						fprintf(stderr, ", \n");
					}
				}
*/				detzone_ns->internal.last_written_blks = detzone_ns->internal.total_written_blks;
			}
 			fprintf(stderr, "\n");
		}

		if (detzone_ctrlr->internal.active_write_zones) {
			avg_write_lat_tsc = detzone_ctrlr->internal.total_avg_write_blk_tsc
														/ detzone_ctrlr->internal.active_write_zones;
			//if (counter & ((1 << 10) - 1) == 0) {
			// if (1) {
			// 	uint64_t ticks_hz = spdk_get_ticks_hz();
			// 	counter = 0;
			// 	fprintf(stderr, "Update lat thresh: active_write_zones(%u) (avg blk lat: %llu us) %llu us --> %llu us \n",
			// 			detzone_ctrlr->internal.active_write_zones,
			// 			avg_write_lat_tsc * SPDK_SEC_TO_USEC / ticks_hz,
			// 			detzone_ctrlr->blk_latency_thresh_ticks * SPDK_SEC_TO_USEC / ticks_hz,
			// 			((avg_write_lat_tsc + detzone_ctrlr->blk_latency_thresh_ticks) >> 1) * SPDK_SEC_TO_USEC / ticks_hz);

			// }
			detzone_ctrlr->blk_latency_thresh_ticks =
						(avg_write_lat_tsc + detzone_ctrlr->blk_latency_thresh_ticks) >> 1;
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

	detzone_ctrlr->internal.active_write_zones = 0;
	detzone_ctrlr->internal.total_avg_write_blk_tsc = 0;

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
	detzone_ctrlr->mgmt_poller = SPDK_POLLER_REGISTER(vbdev_detzone_poller_wr_sched, detzone_ctrlr, 1000);
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
		goto error_out;
	}

	// We don't use the first zone (zone_id == 0) for future use.
	// Also, it makes the zone validation easy as no allocated zone has zone_id 0
	for (zone_idx = DETZONE_RESERVED_ZONES; zone_idx < detzone_ctrlr->num_zones; zone_idx++)
	{
		//detzone_ctrlr->zone_info[zone_idx].state = ctx->ext_info[zone_idx].state;
		//detzone_ctrlr->zone_info[zone_idx].write_pointer = ctx->ext_info[zone_idx].write_pointer;
		//detzone_ctrlr->zone_info[zone_idx].zone_id = ctx->ext_info[zone_idx].zone_id;
		//detzone_ctrlr->zone_info[zone_idx].capacity = ctx->ext_info[zone_idx].capacity;
		detzone_ctrlr->zone_info[zone_idx].pu_group = detzone_ctrlr->num_pu;
		detzone_ctrlr->zone_info[zone_idx].ns_id = 0;
		detzone_ctrlr->zone_info[zone_idx].lzone_id = 0;
		detzone_ctrlr->zone_info[zone_idx].stripe_id = 0;
		detzone_ctrlr->zone_info[zone_idx].stripe_group = empty_stripe_group;
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
				detzone_ctrlr->zone_info[zone_idx].state = SPDK_BDEV_ZONE_STATE_IMP_OPEN;
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
				detzone_ctrlr->zone_info[zone_idx].stripe_group = ctx->zone_md[zone_idx].stripe_group;
				if (ctx->zone_md[zone_idx].version != DETZONE_NS_META_FORMAT_VER) {
					SPDK_ERRLOG("Not supported metadata format version %u\n", ctx->zone_md[zone_idx].version);
					goto error_out;
				}
				// this means nothing, but just increase the alloc counter
				detzone_ctrlr->zone_alloc_cnt++;
			}
			break;
		case SPDK_BDEV_ZONE_STATE_FULL:
				detzone_ctrlr->zone_info[zone_idx].ns_id = ctx->zone_md[zone_idx].ns_id;
				detzone_ctrlr->zone_info[zone_idx].lzone_id = ctx->zone_md[zone_idx].lzone_id;
				detzone_ctrlr->zone_info[zone_idx].stripe_id = ctx->zone_md[zone_idx].stripe_id;
				detzone_ctrlr->zone_info[zone_idx].stripe_group = ctx->zone_md[zone_idx].stripe_group;
				if (ctx->zone_md[zone_idx].version != DETZONE_NS_META_FORMAT_VER) {
					SPDK_ERRLOG("Not supported metadata format version %u\n", ctx->zone_md[zone_idx].version);
					goto error_out;
				}
				
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
	return;

error_out:
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

	for (uint32_t zone_idx = 0; zone_idx < detzone_ctrlr->num_zones; zone_idx++)
	{
		detzone_ctrlr->zone_info[zone_idx].state = ctx->ext_info[zone_idx].state;
		if (detzone_ctrlr->zone_info[zone_idx].state == SPDK_BDEV_ZONE_STATE_EMPTY) {
			detzone_ctrlr->zone_info[zone_idx].write_pointer = ctx->ext_info[zone_idx].zone_id;
		} else if (detzone_ctrlr->zone_info[zone_idx].state == SPDK_BDEV_ZONE_STATE_FULL) {
			detzone_ctrlr->zone_info[zone_idx].write_pointer = ctx->ext_info[zone_idx].zone_id
																+ ctx->ext_info[zone_idx].capacity;
		} else {
			detzone_ctrlr->zone_info[zone_idx].write_pointer = ctx->ext_info[zone_idx].write_pointer;
		}
		detzone_ctrlr->zone_info[zone_idx].zone_id = ctx->ext_info[zone_idx].zone_id;
		detzone_ctrlr->zone_info[zone_idx].capacity = ctx->ext_info[zone_idx].capacity;
		if (_detzone_is_active_state(detzone_ctrlr->zone_info[zone_idx].state)) {
			detzone_ctrlr->num_zone_active += 1;
		}
		if (_detzone_is_open_state(detzone_ctrlr->zone_info[zone_idx].state)) {
			detzone_ctrlr->num_zone_open += 1;
		}
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

		TAILQ_INIT(&detzone_ctrlr->internal.zone_alloc_queued);
		TAILQ_INIT(&detzone_ctrlr->internal.zone_shrink_queued);

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
		detzone_ctrlr->mgmt_bdev.max_open_zones = 256; //bdev->max_open_zones;
		detzone_ctrlr->mgmt_bdev.max_active_zones = 256; //bdev->max_active_zones;
		detzone_ctrlr->mgmt_bdev.optimal_open_zones = 256; //bdev->optimal_open_zones;

		//detzone_ctrlr->max_ns = (detzone_ctrlr->mgmt_bdev.max_active_zones >> 1) / DETZONE_NS_MAX_ACTIVE_ZONE;
		detzone_ctrlr->max_ns = 4;
		detzone_ctrlr->max_ns = spdk_min(detzone_ctrlr->max_ns, DETZONE_MAX_NAMESPACES);
		// make base_essential a power of 2
		detzone_ctrlr->base_essentials = 1u << spdk_u32log2((detzone_ctrlr->mgmt_bdev.max_active_zones >> 1) / detzone_ctrlr->max_ns);
		detzone_ctrlr->base_width = detzone_ctrlr->base_essentials / DETZONE_NS_MAX_ACTIVE_ZONE;
		detzone_ctrlr->avail_spares = detzone_ctrlr->mgmt_bdev.max_active_zones
									   - (detzone_ctrlr->base_essentials * detzone_ctrlr->max_ns);
		detzone_ctrlr->base_spares = detzone_ctrlr->avail_spares / detzone_ctrlr->max_ns;
		detzone_ctrlr->base_stripe_blks = (32 * 1024) / detzone_ctrlr->base_bdev->blocklen;

		detzone_ctrlr->mgmt_bdev.ctxt = detzone_ctrlr;
		detzone_ctrlr->mgmt_bdev.fn_table = &vbdev_detzone_fn_table;
		detzone_ctrlr->mgmt_bdev.module = &detzone_if;

		detzone_ctrlr->num_pu = assoc->num_pu;
		detzone_ctrlr->num_zone_active = 0;
		detzone_ctrlr->num_zone_open = 0;
		detzone_ctrlr->zone_alloc_cnt = 0;
		detzone_ctrlr->num_zones = spdk_bdev_get_num_zones(bdev);
		detzone_ctrlr->zone_info = calloc(detzone_ctrlr->num_zones, sizeof(struct vbdev_detzone_zone_info));

		/* I/O scheduler specific parameters */
		detzone_ctrlr->blk_latency_thresh_ticks = spdk_get_ticks_hz() * 100 / SPDK_SEC_TO_USEC;
		detzone_ctrlr->per_zone_mdts = (128*1024UL) / detzone_ctrlr->mgmt_bdev.blocklen;	// 128KB
		detzone_ctrlr->wr_lat_max_thresh = 1000 * spdk_get_ticks_hz() / SPDK_SEC_TO_USEC;

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
					uint64_t num_base_zones)
{
	struct ns_association *assoc;
	int rc = 0;

	rc = vbdev_detzone_ns_insert_association(detzone_name, ns_name,
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

/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation. All rights reserved.
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

enum vbdev_congctrl_iosched_type {
	IOSCHED_DRR,
	IOSCHED_WDRR,
	IOSCHED_NOOP
};

static void *vbdev_congctrl_iosched_noop_init(void *ctx)
{
	return ctx;
}

static void vbdev_congctrl_iosched_noop_destroy(void *ctx)
{
	return;
}

static void vbdev_congctrl_iosched_noop_enqueue(void *ctx, void *item)
{
	int status;
	struct congctrl_bdev_io *io_ctx = item;
	struct spdk_bdev_io     *bdev_io = spdk_bdev_io_from_ctx(io_ctx);

	return;
}

static void *vbdev_congctrl_iosched_noop_dequeue(void *ctx)
{
	return NULL;
}

static void vbdev_congctrl_iosched_noop_flush(void *ctx)
{
	return;
}

static void vbdev_congctrl_iosched_noop_qpair_destroy(void *ctx)
{
	return;
}

static const struct vbdev_congctrl_iosched_ops noop_ioched_ops = {
	.init 			= vbdev_congctrl_iosched_noop_init,
	.destroy 		= vbdev_congctrl_iosched_noop_destroy,
	.enqueue 		= vbdev_congctrl_iosched_noop_enqueue,
	.dequeue 		= vbdev_congctrl_iosched_noop_dequeue,
	.flush			= vbdev_congctrl_iosched_noop_flush,
};

#define SPDK_NVMF_TMGR_DRR_NUM_VQE (8+1)
#define SPDK_NVMF_TMGR_DRR_MAX_VQE_SIZE (131072)

struct vbdev_congctrl_iosched_drr_vqe {
	uint16_t			id;
	uint16_t			submits;
	uint16_t			completions;
	uint32_t			length;

	TAILQ_ENTRY(vbdev_congctrl_iosched_drr_vqe)	link;
};

struct vbdev_congctrl_iosched_drr_qpair_ctx {
	int		paused;

	uint64_t	round;
	uint32_t 	quantum;
	int64_t 	deficit;
	uint64_t 	processing;
	uint64_t 	backlog;
	int16_t		vqsize;

	uint64_t	vqcount;
	//uint64_t	vqbytes;
	//uint64_t	vqticks;

	uint32_t	idle_count;
	uint32_t	deferred_count;
	uint64_t	deferred_enter;

	int32_t	credit_per_vslot;
	int32_t io_wait;
	int32_t io_processing;

	struct vbdev_congctrl_iosched_drr_sched_ctx *sched_ctx;
	struct vbdev_congctrl_iosched_drr_vqe 	vqe[SPDK_NVMF_TMGR_DRR_NUM_VQE];
	struct vbdev_congctrl_iosched_drr_vqe	*curr_vqe;
	TAILQ_HEAD(, vbdev_congctrl_iosched_drr_vqe)		vq;
	TAILQ_HEAD(, spdk_nvmf_request)		queued;
	
	uint64_t 	temp_data;
};

struct vbdev_congctrl_iosched_drr_sched_ctx {
	struct spdk_nvmf_tmgr *tmgr;

	uint32_t 	num_active_qpairs;
	uint32_t 	num_deferred_qpairs;

	uint64_t	global_quantum;
	uint64_t	target_rate;
	uint64_t	rate_window_ticks;
	uint64_t	last_window_ticks;
	uint32_t	deficit_incr_count;

	uint64_t	last_sched_tsc;
	int16_t		vqmaxqd;

	struct spdk_nvmf_qpair 				*curr_qpair;	
	TAILQ_HEAD(, spdk_nvmf_qpair)			active_qpairs;
	TAILQ_HEAD(, spdk_nvmf_qpair)			deferred_qpairs;
	TAILQ_HEAD(, spdk_nvmf_qpair)			idle_qpairs;
};

static void *vbdev_congctrl_iosched_wdrr_init(void *ctx)
{
	struct spdk_nvmf_tmgr *tmgr = ctx;
	struct vbdev_congctrl_iosched_drr_sched_ctx *sched_ctx;

	sched_ctx = calloc(1, sizeof(struct vbdev_congctrl_iosched_drr_sched_ctx));
	sched_ctx->tmgr = tmgr;
	sched_ctx->num_active_qpairs = 0;
	sched_ctx->vqmaxqd = 1;
	sched_ctx->curr_qpair = NULL;
	TAILQ_INIT(&sched_ctx->active_qpairs);
	TAILQ_INIT(&sched_ctx->deferred_qpairs);
	TAILQ_INIT(&sched_ctx->idle_qpairs);
	return sched_ctx;
}

static void vbdev_congctrl_iosched_wdrr_destroy(void *ctx)
{
	struct vbdev_congctrl_iosched_drr_sched_ctx *sched_ctx = ctx;
	struct spdk_nvmf_tmgr		*tmgr = sched_ctx->tmgr;
	struct spdk_nvmf_qpair		*qpair;

	while (!TAILQ_EMPTY(&sched_ctx->active_qpairs)) {
		qpair = TAILQ_FIRST(&sched_ctx->active_qpairs);
		TAILQ_REMOVE(&sched_ctx->active_qpairs, qpair, iosched_link);
		vbdev_congctrl_iosched_wdrr_qpair_destroy(qpair);
	}
	while (!TAILQ_EMPTY(&sched_ctx->deferred_qpairs)) {
		qpair = TAILQ_FIRST(&sched_ctx->deferred_qpairs);
		TAILQ_REMOVE(&sched_ctx->deferred_qpairs, qpair, iosched_link);
		vbdev_congctrl_iosched_wdrr_qpair_destroy(qpair);
	}
	while (!TAILQ_EMPTY(&sched_ctx->idle_qpairs)) {
		qpair = TAILQ_FIRST(&sched_ctx->idle_qpairs);
		TAILQ_REMOVE(&sched_ctx->idle_qpairs, qpair, iosched_link);
		vbdev_congctrl_iosched_wdrr_qpair_destroy(qpair);
	}
		
	if (tmgr->sched_ctx) {
		free(tmgr->sched_ctx);
		tmgr->sched_ctx = NULL;
	}

	return;
}

#define vbdev_congctrl_iosched_WDRR_VSLOT_CREDIT_MAX 32

static void vbdev_congctrl_iosched_wdrr_enqueue(void *ctx, void *item)
{
	struct vbdev_congctrl_iosched_drr_sched_ctx *sched_ctx = ctx;
	struct spdk_nvmf_request 	*req = item;
	struct spdk_nvmf_tmgr		*tmgr = sched_ctx->tmgr;
	struct spdk_nvmf_qpair		*qpair = req->qpair;
	struct vbdev_congctrl_iosched_drr_qpair_ctx *qpair_ctx;
	struct vbdev_congctrl_iosched_drr_vqe		*vqe;
	int 	i;

	if (spdk_unlikely(qpair->iosched_ctx == NULL)) {
		qpair->iosched_ctx = calloc(1, sizeof(struct vbdev_congctrl_iosched_drr_qpair_ctx));
		qpair_ctx = qpair->iosched_ctx;
		qpair_ctx->sched_ctx = sched_ctx;
		TAILQ_INIT(&qpair_ctx->vq);
		qpair_ctx->credit_per_vslot = vbdev_congctrl_iosched_WDRR_VSLOT_CREDIT_MAX;
		qpair_ctx->io_wait = 0;
		qpair_ctx->vqsize = 1;
		qpair_ctx->deficit = qpair_ctx->quantum = SPDK_NVMF_TMGR_RATE_QUANTUM;
		qpair_ctx->curr_vqe = &qpair_ctx->vqe[0];
		for (i=0; i < SPDK_NVMF_TMGR_DRR_NUM_VQE; i++) {
			vqe = &qpair_ctx->vqe[i];
			vqe->id = i;
			TAILQ_INSERT_TAIL(&qpair_ctx->vq, vqe, link);
		}

		TAILQ_INIT(&qpair_ctx->queued);
		TAILQ_INSERT_TAIL(&sched_ctx->idle_qpairs, qpair, iosched_link);
	} else {
		qpair_ctx = qpair->iosched_ctx;
	}

	TAILQ_INSERT_TAIL(&qpair_ctx->queued, req, link);
	if (!qpair_ctx->backlog && !qpair_ctx->deferred_enter) {
		TAILQ_REMOVE(&sched_ctx->idle_qpairs, qpair, iosched_link);
		TAILQ_INSERT_TAIL(&sched_ctx->active_qpairs, qpair, iosched_link);
		if (!qpair_ctx->io_processing) {
			sched_ctx->num_active_qpairs++;
			sched_ctx->vqmaxqd = spdk_max(1, (SPDK_NVMF_TMGR_DRR_NUM_VQE-1)/sched_ctx->num_active_qpairs);
			qpair_ctx->deficit += qpair_ctx->quantum;
		}
	}
	qpair_ctx->backlog += req->length;
	qpair_ctx->io_wait++;
	tmgr->io_waiting++;
	spdk_nvmf_tmgr_submit(tmgr);
	return;
}

static void *vbdev_congctrl_iosched_wdrr_dequeue(void *ctx)
{
	struct vbdev_congctrl_iosched_drr_sched_ctx *sched_ctx = ctx;
	struct spdk_nvmf_tmgr		*tmgr = sched_ctx->tmgr;
	struct spdk_nvmf_request 	*req = NULL;
	struct spdk_nvmf_qpair		*qpair, *tmp_qpair;
	struct spdk_nvme_cmd		*cmd;
	struct vbdev_congctrl_iosched_drr_qpair_ctx *qpair_ctx;
	int64_t	weighted_length;
	int64_t write_cost = tmgr->write_cost;

	if (TAILQ_EMPTY(&sched_ctx->active_qpairs)) {
		return NULL;
	}

	TAILQ_FOREACH_SAFE(qpair, &sched_ctx->active_qpairs, iosched_link, tmp_qpair) {
		qpair_ctx = qpair->iosched_ctx;
		qpair_ctx->round++;
		req = TAILQ_FIRST(&qpair_ctx->queued);
		cmd = &req->cmd->nvme_cmd;
		weighted_length = req->length;
		if (cmd->opc == SPDK_NVME_OPC_WRITE) {
			weighted_length = (write_cost * weighted_length) >> SPDK_NVMF_TMGR_WEIGHT_PARAM;
		}

		if (qpair_ctx->deficit < weighted_length) {
			req = NULL;
			qpair_ctx->deficit += qpair_ctx->quantum;
			if (!TAILQ_NEXT(qpair, iosched_link)) { // this is already a tail
				tmp_qpair = qpair;
			} else {
				TAILQ_REMOVE(&sched_ctx->active_qpairs, qpair, iosched_link);
				TAILQ_INSERT_TAIL(&sched_ctx->active_qpairs, qpair, iosched_link);
			}
			continue;
		}

		qpair_ctx->curr_vqe->length += weighted_length;
		qpair_ctx->curr_vqe->submits++;
		qpair_ctx->backlog -= req->length;
		qpair_ctx->processing += req->length;
		qpair_ctx->io_processing++;
		qpair_ctx->io_wait--;
		qpair_ctx->deficit -= weighted_length;
		req->vqe_id = qpair_ctx->curr_vqe->id;
		TAILQ_REMOVE(&qpair_ctx->queued, req, link);

		if (qpair_ctx->curr_vqe->length	// this allows oversized v-slot
					>= SPDK_NVMF_TMGR_DRR_MAX_VQE_SIZE) {
			// Find a new slot if the current request is not fit in the opened slot
			TAILQ_REMOVE(&qpair_ctx->vq, qpair_ctx->curr_vqe, link);
			TAILQ_INSERT_TAIL(&qpair_ctx->vq, qpair_ctx->curr_vqe, link);
			if (sched_ctx->vqmaxqd - qpair_ctx->vqsize <= 0) {
				TAILQ_REMOVE(&sched_ctx->active_qpairs, qpair, iosched_link);
				TAILQ_INSERT_TAIL(&sched_ctx->deferred_qpairs, qpair, iosched_link);
				qpair_ctx->curr_vqe = NULL;
				qpair_ctx->deferred_count++;
				qpair_ctx->deferred_enter = qpair_ctx->deferred_count;
				qpair_ctx->deficit = 0;
			} else {
				qpair_ctx->vqsize++;
				qpair_ctx->vqcount++;
				qpair_ctx->curr_vqe = TAILQ_FIRST(&qpair_ctx->vq);
				assert(qpair_ctx->curr_vqe->length == 0);
			}
		}
		if (!qpair_ctx->deferred_enter && qpair_ctx->backlog == 0) {
			qpair_ctx->deficit = 0;
			TAILQ_REMOVE(&sched_ctx->active_qpairs, qpair, iosched_link);
			TAILQ_INSERT_TAIL(&sched_ctx->idle_qpairs, qpair, iosched_link);
		}
		break;
	}
	return req;
}

static void vbdev_congctrl_iosched_wdrr_flush(void *ctx)
{
	struct vbdev_congctrl_iosched_drr_sched_ctx *sched_ctx = ctx;
	struct spdk_nvmf_qpair		*qpair;
	struct vbdev_congctrl_iosched_drr_qpair_ctx *qpair_ctx;

	TAILQ_FOREACH(qpair, &sched_ctx->active_qpairs, iosched_link) {
			qpair_ctx = qpair->iosched_ctx;
			qpair_ctx->deficit = SPDK_NVMF_TMGR_RATE_QUANTUM;
	}

	TAILQ_FOREACH(qpair, &sched_ctx->deferred_qpairs, iosched_link) {
			qpair_ctx = qpair->iosched_ctx;
			qpair_ctx->deficit = SPDK_NVMF_TMGR_RATE_QUANTUM;
	}

	return;
}

static const struct vbdev_congctrl_iosched_ops wdrr_ioched_ops = {
	.init 			= vbdev_congctrl_iosched_wdrr_init,
	.destroy 		= vbdev_congctrl_iosched_wdrr_destroy,
	.enqueue 		= vbdev_congctrl_iosched_wdrr_enqueue,
	.dequeue 		= vbdev_congctrl_iosched_wdrr_dequeue,
	.flush			= vbdev_congctrl_iosched_wdrr_flush,
};

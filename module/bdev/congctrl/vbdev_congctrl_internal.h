/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
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

#ifndef SPDK_VBDEV_CONGCTRL_INTERNAL_H
#define SPDK_VBDEV_CONGCTRL_INTERNAL_H

#include "spdk/stdinc.h"

#include "spdk/bdev.h"
#include "spdk/bdev_module.h"

enum congctrl_io_type {
	CONGCTRL_IO_NONE,
	CONGCTRL_IO_READ,
	CONGCTRL_IO_WRITE,
	CONGCTRL_IO_MGMT
};

/* Data structures for congestion control */
struct congctrl_sched {
	uint64_t thresh_ticks;
	uint64_t thresh_residue;
	uint64_t ewma_ticks;

	int64_t rate;
	uint64_t last_update_tsc;
	uint64_t max_bucket_size;
	uint64_t tokens;
};

struct congctrl_bdev_io {
	int status;

	uint64_t submit_tick;
	uint64_t completion_tick;

	uint64_t next_offset_blocks;
	uint64_t remain_blocks;
	uint32_t outstanding_split_ios;

	struct spdk_io_channel *ch;
	struct spdk_bdev_io_wait_entry bdev_io_wait;
	
	enum congctrl_io_type type;
	struct congctrl_sched *sched;

	struct iovec child_iovs[32];

	TAILQ_ENTRY(congctrl_bdev_io) link;
};

struct congctrl_io_channel {
	uint32_t 	ch_id;
	struct spdk_io_channel	*base_ch; /* IO channel of base device */

	uint64_t	ch_lzslba;	/* current Logical ZSLBA (only one opened logical ZONE at a time) */
	//struct congctrl_cong	rd_cong;
	//struct congctrl_cong	wr_cong;

	TAILQ_HEAD(, congctrl_bdev_io)	slidewin_wait_queue;
	TAILQ_HEAD(, congctrl_bdev_io)	write_drr_queue;
};

struct congctrl_mgmt_channel {
	struct spdk_io_channel	*base_ch; /* IO channel of base device */

	struct spdk_poller *top_poller;
	struct spdk_poller *io_poller;
};

#define VBDEV_CONGCTRL_EWMA_PARAM 	  4	// Weight = 1/(2^VBDEV_CONGCTRL_EWMA_PARAM)
#define VBDEV_CONGCTRL_LATHRES_EWMA_PARAM 4	// Weight = 1/(2^VBDEV_CONGCTRL_LATHRES_EWMA_PARAM)

enum vbdev_congctrl_rate_state {
	VBDEV_CONGCTRL_RATE_SUBMITTABLE,
	VBDEV_CONGCTRL_RATE_SUBMITTABLE_READONLY,
	VBDEV_CONGCTRL_RATE_DEFERRED,
	VBDEV_CONGCTRL_RATE_OVERLOADED,
	VBDEV_CONGCTRL_RATE_CONGESTION,
	VBDEV_CONGCTRL_RATE_SLOWSTART,
	VBDEV_CONGCTRL_RATE_DRAINING,
	VBDEV_CONGCTRL_RATE_OK
};

#define VBDEV_CONGCTRL_EWMA(ewma, raw, param) \
	((raw >> param) + (ewma) - (ewma >> param))


struct vbdev_congctrl_iosched_ops {
	void*	(*init)(void *ctx);
	void 	(*destroy)(void *ctx);
	int 	(*enqueue)(void *ctx, void *req);
	void*	(*dequeue)(void *ctx);
	void	(*flush)(void *ctx);
 };



struct vbdev_congctrl_ns {
	void					*ctrl;
	struct spdk_bdev		congctrl_ns_bdev;    /* the congctrl ns virtual bdev */

	// Namespace specific configuration parameters
	uint32_t	zone_array_size; /* the nubmer of base zones in logical zone */
	uint32_t	stripe_blocks; /* stripe size (in blocks) */
	uint32_t	block_align;

	uint64_t	zcap;		// Logical Zone Capacity

	TAILQ_ENTRY(vbdev_congctrl_ns)	link;

	uint64_t zone_map[0];
};

/* List of congctrl bdev ctrls and associated info for each. */
struct vbdev_congctrl {
	struct spdk_bdev		*base_bdev; /* the thing we're attaching to */
	struct spdk_bdev_desc	*base_desc; /* its descriptor we get from open */
	struct spdk_bdev		mgmt_bdev;    /* the congctrl mgmt bdev */
	
	uint64_t			upper_read_latency; /* the upper read latency */
	uint64_t			lower_read_latency; /* the lower read latency */
	uint64_t			upper_write_latency; /* the upper write latency */
	uint64_t			lower_write_latency; /* the lower write latency */

	uint64_t			claimed_blockcnt; /* claimed blocks by namespaces */
	
	struct spdk_thread		*thread;    /* thread where base device is opened */

	TAILQ_HEAD(, vbdev_congctrl_ns)	ns;
	TAILQ_ENTRY(vbdev_congctrl)	link;
};


#endif /* SPDK_VBDEV_CONGCTRL_INTERNAL_H */
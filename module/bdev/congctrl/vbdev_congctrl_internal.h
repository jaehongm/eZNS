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
	uint32_t outstanding_stripe_ios;

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
	uint64_t	rd_avail_window;
	uint64_t	wr_avail_window;

	TAILQ_HEAD(, congctrl_bdev_io)	rd_slidewin_queue;
	TAILQ_HEAD(, congctrl_bdev_io)	wr_slidewin_queue;
	TAILQ_HEAD(, congctrl_bdev_io)	write_drr_queue;
};

struct congctrl_mgmt_channel {
	struct spdk_poller *io_poller;
};

#define VBDEV_CONGCTRL_EWMA_PARAM 	  4	// Weight = 1/(2^VBDEV_CONGCTRL_EWMA_PARAM)
#define VBDEV_CONGCTRL_LATHRES_EWMA_PARAM 4	// Weight = 1/(2^VBDEV_CONGCTRL_LATHRES_EWMA_PARAM)
#define VBDEV_CONGCTRL_SLIDEWIN_MAX   (128*1024UL)

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

enum vbdev_congctrl_ns_state {
	VBDEV_CONGCTRL_NS_STATE_ACTIVE,
	VBDEV_CONGCTRL_NS_STATE_CLOSE,
	VBDEV_CONGCTRL_NS_STATE_PENDING
};

struct vbdev_congctrl_ns_zone_info {
	uint64_t			base_zone_id;
	uint64_t			write_pointer;
	uint64_t			capacity;
	uint32_t			pu_group;
	enum spdk_bdev_zone_state	state;

	enum spdk_bdev_zone_state	next_state; /* state to be transitioned next */
};

struct vbdev_congctrl_ns {
	void					*ctrl;
	struct spdk_bdev		congctrl_ns_bdev;    /* the congctrl ns virtual bdev */
	struct spdk_thread 		*thread;  /* thread where the namespace is opened */
	
	bool		active;
	uint32_t	ref;
	uint64_t	num_open_lzones;
	uint64_t	base_zone_size;

	// Namespace specific configuration parameters
	uint32_t	zone_array_size; /* the number of base zones in logical zone */
	uint32_t	stripe_blocks; /* stripe size (in blocks) */
	uint32_t	block_align;

	uint64_t	start_base_zone_id;
	uint64_t	num_base_zones;
	uint64_t	zcap;		// Logical Zone Capacity

	/**
	 * Fields that are used internally by the mgmt bdev.
	 * Namespace functions must not to write to these field directly.
	 * Any function accesses these field should aquire lock first.
	 */
	struct __congctrl_ns_internal {
		pthread_spinlock_t	lock;
		enum vbdev_congctrl_ns_state ns_state;
	} internal;

	TAILQ_ENTRY(vbdev_congctrl_ns)	link;
	TAILQ_ENTRY(vbdev_congctrl_ns)	state_link;

	struct vbdev_congctrl_ns_zone_info zone_info[0];
};

/* List of congctrl bdev ctrls and associated info for each. */
struct vbdev_congctrl {
	struct spdk_bdev		*base_bdev; /* the thing we're attaching to */
	struct spdk_bdev_desc	*base_desc; /* its descriptor we get from open */
	struct spdk_bdev		mgmt_bdev;    /* the congctrl mgmt bdev */
	struct spdk_io_channel  *mgmt_ch;	/* the congctrl mgmt channel */
	
	uint32_t			num_pu;			  /* the number of parallel units (NAND dies) in the SSD */
	uint64_t 			zone_alloc_cnt;		  /* zone allocation counter */ 
	uint64_t			claimed_blockcnt; /* claimed blocks by namespaces */
	uint64_t			num_open_states;		/* the number of open state zones granted to namespaces */ 
	struct spdk_thread		*thread;    /* thread where base device is opened */

	TAILQ_HEAD(, vbdev_congctrl_ns)	ns;
	TAILQ_HEAD(, vbdev_congctrl_ns)	ns_active;
	TAILQ_HEAD(, vbdev_congctrl_ns)	ns_pending;
	TAILQ_ENTRY(vbdev_congctrl)	link;
};

struct vbdev_congctrl_ns_mgmt_io_ctx {
	int status;

	uint32_t remain_ios;
	uint32_t outstanding_mgmt_ios;

	struct {
		/* First logical block of a zone */
		uint64_t zone_id;

		/* Number of zones */
		uint32_t num_zones;

		/* Used to change zoned device zone state */
		enum spdk_bdev_zone_action zone_action;

		/* The data buffer */
		void *buf;
	} zone_mgmt;

	struct {
		int sct;
		int sc;
	} nvme_status;
	struct spdk_bdev_io 		*parent_io;
	struct vbdev_congctrl_ns 	*congctrl_ns;
};

enum vbdev_congctrl_zns_specific_status_code {
	SPDK_NVME_SC_ZONE_BOUNDARY_ERROR = 0xB8,
	SPDK_NVME_SC_ZONE_IS_FULL		= 0xB9,
	SPDK_NVME_SC_ZONE_IS_READONLY	= 0xBA,
	SPDK_NVME_SC_ZONE_IS_OFFLINE	= 0xBB,
	SPDK_NVME_SC_ZONE_INVALID_WRITE	= 0xBC,
	SPDK_NVME_SC_ZONE_TOO_MANY_ACTIVE	= 0xBD,
	SPDK_NVME_SC_ZONE_TOO_MANY_OPEN		= 0xBE,
	SPDK_NVME_SC_ZONE_INVALID_STATE		= 0xBF
};

#endif /* SPDK_VBDEV_CONGCTRL_INTERNAL_H */
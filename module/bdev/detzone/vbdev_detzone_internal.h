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

#ifndef SPDK_VBDEV_DETZONE_INTERNAL_H
#define SPDK_VBDEV_DETZONE_INTERNAL_H

#include "spdk/bdev.h"
#include "spdk/bdev_module.h"

enum detzone_io_type {
	DETZONE_IO_NONE = 0,
	DETZONE_IO_READ = 1,
	DETZONE_IO_WRITE = 1 << 1,
	DETZONE_IO_APPEND = 1 << 2,
	DETZONE_IO_MGMT = 1 << 3
};

typedef void (*detzone_ns_mgmt_completion_cb)(struct spdk_bdev_io *bdev_io,
		int sct, int sc,
		void *cb_arg);

struct vbdev_detzone_ns_mgmt_io_ctx {
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

		/* Select All flag */
		bool select_all;

		/* The data buffer */
		void *buf;
	} zone_mgmt;

	struct {
		/** NVMe completion queue entry DW0 */
		uint32_t cdw0;
		/** NVMe status code type */
		int sct;
		/** NVMe status code */
		int sc;
	} nvme_status;

	struct spdk_bdev_io 		*parent_io;
	struct spdk_thread			*submited_thread;
	struct vbdev_detzone_ns 	*detzone_ns;

	detzone_ns_mgmt_completion_cb cb;
	void *cb_arg;
};

struct detzone_bdev_io {
	int status;
	bool is_busy;
	enum detzone_io_type type;

	union {
		struct {
			int iov_idx;
			uint64_t iov_offset;
			uint64_t next_offset_blocks;
			uint64_t remain_blocks;
			uint64_t outstanding_stripe_ios;
		} io;

		struct {
			uint32_t remain_ios;
			uint32_t outstanding_mgmt_ios;

			struct {
				/* First logical block of a zone */
				uint64_t zone_id;

				/* Number of zones */
				uint32_t num_zones;

				/* Used to change zoned device zone state */
				enum spdk_bdev_zone_action zone_action;

				/* Select All flag */
				bool select_all;

				/* The data buffer */
				void *buf;
			} zone_mgmt;

			struct spdk_bdev_io 		*parent_io;
			struct vbdev_detzone_ns 	*detzone_ns;

			detzone_ns_mgmt_completion_cb cb;
			void *cb_arg;
		} mgmt;
	} u;

	struct spdk_io_channel *ch;
	struct spdk_bdev_io_wait_entry bdev_io_wait;

	struct {
		/** NVMe completion queue entry DW0 */
		uint32_t cdw0;
		/** NVMe status code type */
		int sct;
		/** NVMe status code */
		int sc;
	} nvme_status;

	TAILQ_ENTRY(detzone_bdev_io) link;
};

struct detzone_io_channel {
	uint32_t 	ch_id;
	struct spdk_io_channel	*base_ch; /* IO channel of base device */

	// statistic for the write I/O scheduler
	uint64_t			write_blks;
	uint64_t			total_write_blk_tsc;

	struct spdk_poller	*write_sched_poller; /* credit generator */
};

struct detzone_mgmt_channel {
	struct spdk_poller *io_poller;
};

enum vbdev_detzone_ns_state {
	VBDEV_DETZONE_NS_STATE_ACTIVE,
	VBDEV_DETZONE_NS_STATE_CLOSE,
	VBDEV_DETZONE_NS_STATE_PENDING
};

#define DETZONE_MAX_STRIPE_WIDTH 128
#define DETZONE_RESERVED_ZONES   1
#define DETZONE_RESERVATION_BLKS 1
#define DETZONE_INLINE_META_BLKS 1
#define DETZONE_WRITEV_MAX_IOVS 32

struct vbdev_detzone_ns_zone {
	uint64_t			zone_id;
	uint64_t			write_pointer;
	uint64_t			capacity;
	enum spdk_bdev_zone_state	state;

	struct __detzone_ns_base_zone_info {
		uint64_t						zone_id;
		// vectored I/O
		struct iovec			iovs[DETZONE_WRITEV_MAX_IOVS];
		uint32_t				iov_cnt;
		uint64_t				iov_blks;
	} base_zone[DETZONE_MAX_STRIPE_WIDTH];

	uint32_t					wr_zone_in_progress;
	uint64_t					wr_outstanding_ios;
	uint64_t					tb_tokens;
	uint64_t					tb_last_update_tsc;

	TAILQ_HEAD(, detzone_bdev_io)	wr_pending_queue;
	TAILQ_HEAD(, detzone_bdev_io)	wr_waiting_cpl;

	TAILQ_ENTRY(vbdev_detzone_ns_zone)	link;
};

struct vbdev_detzone_ns {
	void					*ctrl;
	struct spdk_bdev		detzone_ns_bdev;    /* the detzone ns virtual bdev */

	struct spdk_thread 		*wr_thread;  /* thread where the namespace process write IOs */
	struct spdk_io_channel 	*wr_ch;
	
	uint32_t	nsid;
	bool		active;
	uint32_t	ref;
	uint32_t	num_active_zones;
	uint32_t	num_open_zones;
	uint64_t	base_zone_size;

	// Namespace specific configuration parameters
	uint32_t	zone_stripe_width; /* the number of base zones in logical zone */
	uint32_t	zone_stripe_blks; /* stripe size (in blocks) */
	uint32_t	zone_stripe_tb_size;
	uint32_t	block_align;
	uint64_t	zcap;		// Logical Zone Capacity
	uint32_t	padding_blocks;	// num of blocks for padding at the beginning of base zone

	/**
	 * Fields that are used internally by the mgmt bdev.
	 * Namespace functions must not to write to these fields directly.
	 */
	struct __detzone_ns_internal {
		enum vbdev_detzone_ns_state ns_state;
		struct vbdev_detzone_ns_zone *zone;
		struct spdk_bit_array *epoch_pu_map;	// PU allocation bitmap for the current epoch
		uint32_t			   epoch_num_pu;	// PU allocation count for the current epoch

		TAILQ_HEAD(, vbdev_detzone_ns_zone)	zone_write_queue;
	} internal;

	TAILQ_ENTRY(vbdev_detzone_ns)	link;
	TAILQ_ENTRY(vbdev_detzone_ns)	state_link;
};

struct vbdev_detzone_zone_md {
	uint32_t			ns_id;
	uint64_t			lzone_id;
	uint32_t			stripe_id;

	uint32_t			stripe_width;
	uint32_t			stripe_size;

	char reserved[40];
};

struct vbdev_detzone_zone_info {
	uint64_t			zone_id;
	uint64_t			write_pointer;
	uint64_t			capacity;
	enum spdk_bdev_zone_state	state;

	uint32_t			ns_id;
	uint64_t			lzone_id;
	uint32_t			stripe_id;

	uint32_t			stripe_width;
	uint32_t			stripe_size;

	uint32_t			pu_group;
	TAILQ_ENTRY(vbdev_detzone_zone_info) link;
};

/* List of detzone bdev ctrls and associated info for each. */
struct vbdev_detzone {
	struct spdk_bdev		*base_bdev; /* the thing we're attaching to */
	struct spdk_bdev_desc	*base_desc; /* its descriptor we get from open */
	struct spdk_bdev		mgmt_bdev;    /* the detzone mgmt bdev */
	struct spdk_io_channel  *mgmt_ch;	/* the detzone mgmt channel */
	struct spdk_poller		*mgmt_poller;

	uint32_t			num_pu;			  /* the number of parallel units (NAND dies) */
	uint32_t			num_zone_reserved;	/* the number of reserved zone */
	uint32_t			num_zone_empty;		/* the number of empty zone */
	uint64_t 			zone_alloc_cnt;		  /* zone allocation counter */ 

	uint32_t			per_zone_mdts;

	uint32_t			num_ns;				/* number of namespace */
	uint64_t			claimed_blockcnt; /* claimed blocks by namespaces */
	uint64_t			num_open_states;		/* the number of open state zones granted to namespaces */ 
	struct spdk_thread		*thread;    /* thread where base device is opened */

	uint64_t			num_zones;
	struct vbdev_detzone_zone_info 	*zone_info;

	uint64_t			blk_latency_thresh_ticks;

	struct __detzone_internal {
		// statistic for the write I/O scheduler
		uint64_t			active_channels;
		uint64_t			total_write_blk_tsc;
	} internal;

	TAILQ_HEAD(, vbdev_detzone_ns)	ns;
	TAILQ_HEAD(, vbdev_detzone_ns)	ns_active;
	TAILQ_HEAD(, vbdev_detzone_ns)	ns_pending;

	TAILQ_HEAD(, vbdev_detzone_zone_info) zone_reserved;
	TAILQ_HEAD(, vbdev_detzone_zone_info) zone_empty;

	TAILQ_ENTRY(vbdev_detzone)	link;
};

struct vbdev_detzone_update_ctx {
	struct vbdev_detzone 		 *detzone_ctrlr;
	struct vbdev_detzone_ns		 *detzone_ns;
	struct spdk_bdev_zone_info	 info;

};

typedef void (*detzone_md_io_completion_cb)(void *cb_arg, bool success);

struct vbdev_detzone_md_io_ctx {
	struct vbdev_detzone *detzone_ctrlr;
	struct vbdev_detzone_zone_md *zone_md;
	uint64_t zslba;
	uint64_t remaining_zones;

	void *buf;

	detzone_md_io_completion_cb cb;
	void *cb_arg;
};

enum vbdev_detzone_zns_specific_status_code {
	SPDK_NVME_SC_ZONE_BOUNDARY_ERROR = 0xB8,
	SPDK_NVME_SC_ZONE_IS_FULL		= 0xB9,
	SPDK_NVME_SC_ZONE_IS_READONLY	= 0xBA,
	SPDK_NVME_SC_ZONE_IS_OFFLINE	= 0xBB,
	SPDK_NVME_SC_ZONE_INVALID_WRITE	= 0xBC,
	SPDK_NVME_SC_ZONE_TOO_MANY_ACTIVE	= 0xBD,
	SPDK_NVME_SC_ZONE_TOO_MANY_OPEN		= 0xBE,
	SPDK_NVME_SC_ZONE_INVALID_STATE		= 0xBF
};

#endif /* SPDK_VBDEV_DETZONE_INTERNAL_H */
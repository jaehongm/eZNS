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

#ifndef SPDK_VBDEV_CONGCTRL_H
#define SPDK_VBDEV_CONGCTRL_H

#include "spdk/stdinc.h"

#include "spdk/bdev.h"
#include "spdk/bdev_module.h"

struct vbdev_congctrl;

/**
 * Create new congctrl ns.
 *
 * \param congctrl_name Name of the congctrl ctrl on which ns will be created.
 * \param ns_name Name of the congctrl ns.
 * \return 0 on success, other on failure.
 */
int create_congctrl_ns(const char *congctrl_name, const char *ns_name,
					uint32_t zone_array_size, uint32_t stripe_size, uint32_t block_align,
					uint64_t start_zone_id, uint64_t num_phys_zones);

/**
 * Delete congctrl ns.
 *
 * \param bdev Pointer to congctrl ns bdev.
 * \param cb_fn Function to call after deletion.
 * \param cb_arg Argument to pass to cb_fn.
 */
void delete_congctrl_ns(struct spdk_bdev *bdev, spdk_bdev_unregister_cb cb_fn,
					void *cb_arg);


/**
 * Create new congctrl ctrl.
 *
 * \param bdev_name Bdev on which congctrl vbdev will be created.
 * \param vbdev_name Name of the congctrl bdev.
 * \param upper_read_latency Desired upperbound read latency.
 * \param lower_read_latency Desired lowerbound read latency
 * \param upper_write_latency Desired upperbound write latency.
 * \param lower_write_latency Desired lowerbound write latency
 * \return 0 on success, other on failure.
 */
int create_congctrl_disk(const char *bdev_name, const char *vbdev_name, uint64_t upper_read_latency,
		      uint64_t lower_read_latency, uint64_t upper_write_latency, uint64_t lower_write_latency);

/**
 * Delete congctrl ctrl.
 *
 * \param bdev Pointer to congctrl bdev.
 * \param cb_fn Function to call after deletion.
 * \param cb_arg Argument to pass to cb_fn.
 */
void delete_congctrl_disk(struct spdk_bdev *bdev, spdk_bdev_unregister_cb cb_fn,
		       void *cb_arg);

/**
 * Update one of the latency values for a given congctrl bdev.
 *
 * \param congctrl_name The name of the congctrl bdev
 * \param latency_us The new latency threshold value, in microseconds
 * \param type a valid value from the congctrl_io_type enum
 * \return 0 on success, -ENODEV if the bdev cannot be found, and -EINVAL if the bdev is not a congctrl device.
 */
int vbdev_congctrl_update_latency_value(char *congctrl_name, uint64_t latency_upper, uint64_t latency_lower,
				     enum congctrl_io_type type);

#endif /* SPDK_VBDEV_CONGCTRL_H */

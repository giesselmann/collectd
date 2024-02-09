/**
 * collectd - src/target_sma.c
 * Copyright (C) 2008-2009  Florian Forster
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * Authors:
 *   Pay Giesselmann <giesselmann at dkrz.de>
 **/

#include "collectd.h"

#include "filter_chain.h"
#include "utils/common/common.h"

struct tsma_data_s {
  int window;
  int *window_ptr;
  double *window_buffer;

  char **data_sources;
  size_t data_sources_num;
};
typedef struct tsma_data_s tsma_data_t;

static int tsma_invoke_gauge(const data_set_t *ds, value_list_t *vl, /* {{{ */
                           tsma_data_t *data, int dsrc_index) {
  const int window = data->window;
  const int window_offset = dsrc_index * window;
  // store new value at pos of oldest
  data->window_buffer[window_offset + data->window_ptr[dsrc_index]] = vl->values[dsrc_index].gauge;
  data->window_ptr[dsrc_index] = (data->window_ptr[dsrc_index] + 1) % window;
  // average of current window
  double window_sum = 0.0;
  for (int i = window_offset; i < window_offset + window; i++) {
    window_sum += data->window_buffer[i];
  }
  vl->values[dsrc_index].gauge = window_sum / (double)window;
  return 0;
} /* }}} int tsma_invoke_gauge */

static int tsma_config_set_int(int *ret, oconfig_item_t *ci) /* {{{ */
{
  if ((ci->values_num != 1) || (ci->values[0].type != OCONFIG_TYPE_NUMBER)) {
    WARNING("sma target: The `%s' config option needs "
            "exactly one numeric argument.",
            ci->key);
    return -1;
  }

  *ret = ci->values[0].value.number;
  DEBUG("tsma_config_set_int: *ret = %g", *ret);

  return 0;
} /* }}} int tsma_config_set_int */

static int tsma_config_add_data_source(tsma_data_t *data, /* {{{ */
                                     oconfig_item_t *ci) {
  size_t new_data_sources_num;
  char **temp;

  /* Check number of arguments. */
  if (ci->values_num < 1) {
    ERROR("`value' match: `%s' needs at least one argument.", ci->key);
    return -1;
  }

  /* Check type of arguments */
  for (int i = 0; i < ci->values_num; i++) {
    if (ci->values[i].type == OCONFIG_TYPE_STRING)
      continue;

    ERROR("`value' match: `%s' accepts only string arguments "
          "(argument %i is a %s).",
          ci->key, i + 1,
          (ci->values[i].type == OCONFIG_TYPE_BOOLEAN) ? "truth value"
                                                       : "number");
    return -1;
  }

  /* Allocate space for the char pointers */
  new_data_sources_num = data->data_sources_num + ((size_t)ci->values_num);
  temp = realloc(data->data_sources, new_data_sources_num * sizeof(char *));
  if (temp == NULL) {
    ERROR("`value' match: realloc failed.");
    return -1;
  }
  data->data_sources = temp;

  /* Copy the strings, allocating memory as needed.  */
  for (int i = 0; i < ci->values_num; i++) {
    size_t j;

    /* If we get here, there better be memory for us to write to.  */
    assert(data->data_sources_num < new_data_sources_num);

    j = data->data_sources_num;
    data->data_sources[j] = sstrdup(ci->values[i].value.string);
    if (data->data_sources[j] == NULL) {
      ERROR("`value' match: sstrdup failed.");
      continue;
    }
    data->data_sources_num++;
  }

  return 0;
} /* }}} int tsma_config_add_data_source */

static int tsma_destroy(void **user_data) /* {{{ */
{
  tsma_data_t *data;

  if (user_data == NULL)
    return -EINVAL;

  data = (tsma_data_t *)*user_data;

  if ((data != NULL) && (data->data_sources != NULL)) {
    for (size_t i = 0; i < data->data_sources_num; i++)
      sfree(data->data_sources[i]);
    sfree(data->data_sources);
  }
  if ((data != NULL) && (data->window_buffer != NULL)) {
    sfree(data->window_buffer);
  }
  if ((data != NULL) && (data->window_ptr != NULL)) {
    sfree(data->window_ptr);
  }

  sfree(data);
  *user_data = NULL;

  return 0;
} /* }}} int tsma_destroy */

static int tsma_create(const oconfig_item_t *ci, void **user_data) /* {{{ */
{
  tsma_data_t *data;
  int status;

  data = calloc(1, sizeof(*data));
  if (data == NULL) {
    ERROR("tsma_create: calloc failed.");
    return -ENOMEM;
  }

  data->window = 1;
  data->window_buffer = NULL;
  data->window_ptr = NULL;

  status = 0;
  for (int i = 0; i < ci->children_num; i++) {
    oconfig_item_t *child = ci->children + i;

    if (strcasecmp("Window", child->key) == 0)
      status = tsma_config_set_int(&data->window, child);
    else if (strcasecmp("DataSource", child->key) == 0)
      status = tsma_config_add_data_source(data, child);
    else {
      ERROR("Target `sma': The `%s' configuration option is not understood "
            "and will be ignored.",
            child->key);
      status = 0;
    }

    if (status != 0)
      break;
  }

  if (status != 0) {
    tsma_destroy((void *)&data);
    return status;
  }

  *user_data = data;
  return 0;
} /* }}} int tsma_create */

static int tsma_invoke(const data_set_t *ds, value_list_t *vl, /* {{{ */
                     notification_meta_t __attribute__((unused)) * *meta,
                     void **user_data) {
  tsma_data_t *data;

  if ((ds == NULL) || (vl == NULL) || (user_data == NULL))
    return -EINVAL;

  data = *user_data;
  if (data == NULL) {
    ERROR("Target `sma': Invoke: `data' is NULL.");
    return -EINVAL;
  }
  // allocate buffer
  if (data->window_buffer == NULL) {
    data->window_ptr = calloc(1, data->window * sizeof(int));
    data->window_buffer = calloc(ds->ds_num, data->window * sizeof(double));
    if ((data->window_buffer == NULL) || (data->window_ptr == NULL)) {
      ERROR("Target `sma': Failed to allocated buffer");
      return -EINVAL;
    }
    // initialize windows to all zero
    for (int i=0; i<ds->ds_num * data->window; i++){
      data->window_buffer[i] = 0.0;
    }
  }

  for (size_t i = 0; i < ds->ds_num; i++) {
    /* If we've got a list of data sources, is it in the list? */
    if (data->data_sources) {
      size_t j;
      for (j = 0; j < data->data_sources_num; j++)
        if (strcasecmp(ds->ds[i].name, data->data_sources[j]) == 0)
          break;

      /* No match, ignore */
      if (j >= data->data_sources_num)
        continue;
    }

    if (ds->ds[i].type == DS_TYPE_GAUGE)
      tsma_invoke_gauge(ds, vl, data, i);
    else
      ERROR("Target `sma': Ignoring unknown data source type %i",
            ds->ds[i].type);
  }

  return FC_TARGET_CONTINUE;
} /* }}} int tsma_invoke */

void module_register(void) {
  target_proc_t tproc = {0};

  tproc.create = tsma_create;
  tproc.destroy = tsma_destroy;
  tproc.invoke = tsma_invoke;
  fc_register_target("sma", tproc);
} /* module_register */

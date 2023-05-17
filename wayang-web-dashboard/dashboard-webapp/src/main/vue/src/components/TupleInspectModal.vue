<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
  -->
<template>
    <div class="modal fade" :id="'modal-' + tuple.hackit_tuple.metadata.tuple_id" aria-hidden="true"
      aria-labelledby="modalTitle" tabindex="-1">
      <div class="modal-dialog modal-dialog-centered">
        <div class="modal-content">
          <div class="modal-header">
            <h5 class="modal-title" id="modalTitle">Tuple Details - {{ tuple.hackit_tuple.metadata.tuple_id }}</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
          </div>
          <div class="modal-body">
            <table class="table">
              <tbody>
                <tr>
                  <th style="border: none;">Job ID</th>
                  <td style="border: none;">
                    <input id="job_id" :value="tuple.job_id" disabled readonly class="form-control" />
                  </td>
                </tr>
                <tr>
                  <th style="border: none;">Timestamp</th>
                  <td style="border: none;">
                    <input id="timestamp" :value="formatTimestamp(tuple.hackit_tuple.metadata.timestamp)" disabled readonly
                      class="form-control" />
                  </td>
                </tr>
                <tr>
                  <th style="border: none;">Tuple ID</th>
                  <td style="border: none;">
                    <input id="tuple_id" :value="tuple.hackit_tuple.metadata.tuple_id" disabled readonly
                      class="form-control" />
                  </td>
                </tr>
                <tr>
                  <th style="border: none;">Original Tuple</th>
                  <td style="border: none;">
                    <input id="tuple_id" :value="JSON.stringify(tuple.hackit_tuple.wayang_tuple)" disabled readonly
                      class="form-control" />
                  </td>
                </tr>
                <tr>
                  <th style="border: none;">Wayang Tuple</th>
                  <td style="border: none;">
                    <table class="table" :class="{ 'table-warning': debug }">
                      <thead>
                        <tr>
                          <th>Key</th>
                          <th>Value</th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr v-for="(value, key) in tuple.hackit_tuple.wayang_tuple" :key="key">
                          <td>{{ key }}</td>
                          <td>
                            <input v-if="debug" class="form-control" :value="value" :readonly="!debug" />
                            <input v-else class="form-control" :value="value" disabled readonly />
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </td>
                </tr>
  
                <tr>
                  <th>TAGS</th>
                  <td>
                    <div>
                      <span v-for="tag in tuple.hackit_tuple.metadata.tags" :key="tag"
                        class="badge bg-secondary text-white me-1">{{ tag }}</span>
                    </div>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
  
  
          <div class="modal-footer">
            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
            <button v-if="tuple.hackit_tuple.metadata.tags.includes('DEBUG')" type="button"
              class="btn btn-primary">Done</button>
          </div>
        </div>
      </div>
    </div>
  </template>
  <script>
  export default {
    name: 'TupleInspectModal',
    props: {
      tuple: Object,
    },
    methods: {
      formatTimestamp(timestamp) {
        const date = new Date(timestamp);
        return date.toLocaleString();
      },
    },
    computed: {
      debug: function () {
        return this.tuple.hackit_tuple.metadata.tags.includes('DEBUG');
      }
    }
  };
  </script>
  
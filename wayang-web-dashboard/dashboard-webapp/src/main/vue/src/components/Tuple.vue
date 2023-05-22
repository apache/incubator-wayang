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
  <tr class="tuple">
    <td>
      {{ formatTimestamp(tuple.hackit_tuple.metadata.timestamp) }}
    </td>
    <td>
      <span class="d-inline-block text-truncate" style="max-width: 150px;">
        {{ tuple.task_id }}
      </span>
    </td>
    <td>{{ tuple.hackit_tuple.metadata.tuple_id }}</td>
    <td>
      <span class="d-inline-block text-truncate" style="max-width: 200px;">
        {{ tuple.hackit_tuple.wayang_tuple }}
      </span>
    </td>
    <td>
      <i v-if="tuple.hackit_tuple.metadata.tags.includes('MONITOR')" class="fas fa-magnifying-glass"></i>
      <i v-if="tuple.hackit_tuple.metadata.tags.includes('DEBUG')" class="fas fa-bug red-icon text-danger"></i>
    </td>
    <td>
      <button class="btn btn-warning btn-sm rounded-0" data-bs-toggle="modal"
        :data-bs-target="'#modal-' + tuple.hackit_tuple.metadata.tuple_id">
        Inspect
      </button>
    </td>
    <tuple-inspect-modal :tuple="tuple"></tuple-inspect-modal>
  </tr>
</template>

<script>
import TupleInspectModal from "./TupleInspectModal.vue";

export default {
  name: "Tuple",

  props: {
    tuple: Object,
  },
  data() {
    return {};
  },
  methods: {
    formatTimestamp(timestamp) {
      const date = new Date(timestamp);
      return date.toLocaleString();
    },
  },
  components: {
    TupleInspectModal,
  },
  computed: {
    debug: function () {
      return this.tuple.hackit_tuple.metadata.tags.includes("DEBUG");
    },
  },
};
</script>

<style scoped></style>

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
  <div class="hackit-debugger">
    <div v-if="isLoading">Loading...</div>
    <div v-else>
      <div class="filters">
        <label for="task-select">Filter by Task ID:</label>
        <select id="task-select" v-model="selectedTask">
          <option value="">All</option>
          <option v-for="taskId in taskIds" :key="taskId" :value="taskId">{{ taskId }}</option>
        </select>
        <label for="tag-select">Filter by Tag:</label>
        <select id="tag-select" v-model="selectedTag">
          <option value="">All</option>
          <option v-for="tag in uniqueTags" :key="tag" :value="tag">{{ tag }}</option>
        </select>
      </div>
      <table class="table">
        <thead>
          <tr>
            <th>Timestamp</th>
            <th>Task ID</th>
            <th>Tuple ID</th>
            <th>Wayang Tuple</th>
            <th>Hackit Tags</th>
            <th></th>
          </tr>
        </thead>
        <tbody v-if="filteredTuples.length > 0">
          <Tuple v-for="(tuple, index) in filteredTuples" :key="index" :tuple="tuple" />
        </tbody>
        <div class="alert alert-warning" role="alert" v-else>
          No tuples available in this task
        </div>
      </table>
    </div>
  </div>
</template>


<script>
import Tuple from './Tuple.vue';

export default {
  name: 'HackitDebugger',
  components: {
    Tuple,
  },

  props: {
    jobId: {
      type: String,
      required: true,
    },
    taskId: {
      type: String,
      required: true,
    },
    hackitAction: {
      type: String,
      required: false
    }
  },

  data() {
    return {
      isLoading: true,
      tuples: [],
      selectedTask: '',
      selectedTag: '',
    };
  },

  created() {
    this.fetchTuples();
  },

  watch: {
    taskId: function () {
      this.filterTuples();
    },
    hackitAction: function(){
      this.hackitActionRun()
    }
  },

  computed: {
    filteredTuples() {
      let filtered = this.tuples;

      if (this.selectedTask) {
        filtered = filtered.filter((tuple) => tuple.task_id === this.selectedTask);
      }

      if (this.selectedTag) {
        filtered = filtered.filter((tuple) => tuple.hackit_tuple.metadata.tags.includes(this.selectedTag));
      }

      return filtered;
    },

    taskIds() {
      const taskIds = new Set();
      this.tuples.forEach((tuple) => {
        taskIds.add(tuple.task_id);
      });
      return Array.from(taskIds);
    },
    uniqueTags() {
      const tags = new Set();
      this.tuples.forEach((tuple) => {
        tuple.hackit_tuple.metadata.tags.forEach((tag) => tags.add(tag));
      });
      return Array.from(tags);
    },
  },

  methods: {
    async fetchTuples() {
      try {
        const response = await fetch(`http://localhost:3000/tuples/?job_id=${this.jobId}`);
        this.tuples = await response.json();
        this.filterTuples();
      } catch (error) {
        console.error('Error fetching tuples:', error);
      } finally {
        this.isLoading = false;
      }
    },
    hackitActionRun(){
      switch (this.hackitAction) {
        case 'nextTuple':
          this.createNewNextTuple()
          break;
        case '':
          break;
        default:
          break;
      }
    },
    createNewNextTuple(){
      const tempTuples = this.filteredTuples
      const tempTuple = JSON.parse(JSON.stringify( tempTuples[tempTuples.length -1]))
      tempTuple.hackit_tuple.metadata.tuple_id = tempTuples[tempTuples.length -1].hackit_tuple.metadata.tuple_id + 1
      this.tuples.push(tempTuple)
    },
    filterTuples() {
      this.selectedTask = this.taskId;
    },
  },
};
</script>

<style scoped></style>
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
  <tr>
    <td>{{ job.name }}</td>
    <td>{{ formattedDate(job.submitted_on) }}</td>
    <td>{{ formattedDuration }}</td>

    <td>{{ job.end_time ? formattedDate(job.end_time) : "-" }}</td>
    <td><span :class="bgClass">{{ job.stages[0] }}</span><span class="badge bg-dark rounded-0">{{ job.stages[1] }}</span></td>
    <td><span :class="bgClass">{{ job.tasks[0] }}</span><span class="badge bg-dark rounded-0">{{ job.tasks[1] }}</span></td>
    <td>
      <span :class="bgClass">{{ job.status }}</span>
    </td>
    <td>
      <span v-for="plugin in job.plugins" :key="plugin" class="badge bg-info rounded-0 border border-2">{{ plugin }}</span>
    </td>
    <td>
      <router-link class="btn btn-outline-secondary btn-sm rounded-0" :to="'/jobs/' + job.id">Details</router-link>
    </td>
  </tr>
</template>

  
  
  
<script>
export default {
  props: {
    job: {
      type: Object,
      required: true
    }
  },
  methods:{
    formattedDate(timestamp) {
      const date = new Date(timestamp);
      return date.toISOString().split('.')[0];
    },
  },
  computed: {
    formattedDuration() {
      let duration = this.job.duration;
      let seconds = Math.floor(duration / 1000);
      let minutes = Math.floor(seconds / 60);
      seconds = seconds % 60;
      let hours = Math.floor(minutes / 60);
      minutes = minutes % 60;
      let days = Math.floor(hours / 24);
      hours = hours % 24;
      let formattedDuration = "";
      if (days > 0) {
        formattedDuration += days + "d ";
      }
      if (hours > 0) {
        formattedDuration += hours + "h ";
      }
      if (minutes > 0) {
        formattedDuration += minutes + "m ";
      }
      formattedDuration += seconds + "s";
      return formattedDuration;
    },
    bgClass() {
      const status = this.job.status.toLowerCase();
      switch (status) {
        case 'finished':
          return 'badge bg-secondary rounded-0';
        case 'running':
          return 'badge bg-success rounded-0';
        case 'failed':
          return 'badge bg-danger rounded-0';
        case 'canceled':
          return 'badge bg-warning rounded-0';
        default:
          return 'badge bg-info rounded-0';
      }
    }
  },
  data() {
    return {
      isOpen: false,
    };
  },
};
</script>
  
<style scoped></style>
  
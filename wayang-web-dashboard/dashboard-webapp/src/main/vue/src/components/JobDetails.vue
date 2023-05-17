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
  <div class="job-details">
    <table class="table table-striped">
      <tbody>
        <tr>
          <th scope="row">JobID:</th>
          <td>{{ job.id }}</td>
        </tr>
        <tr>
          <th scope="row">Name:</th>
          <td>{{ job.name }}</td>
        </tr>
        <tr>
          <th scope="row">Status:</th>
          <td>{{ job.status }}</td>
        </tr>
        <tr>
          <th scope="row">Submitted On:</th>
          <td>{{ formattedDate(job.submitted_on) }}</td>
        </tr>
        <tr>
          <th scope="row">Duration:</th>
          <td>{{ formattedDuration(job.duration) }}</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>
<script>
export default {
  name: 'JobDetails',
  props: {
    job: {
      type: Object,
      required: true
    }
  },
  data() {
    return {
      showDetails: false,
    };
  },
  methods: {
    formattedDate() {
      const date = new Date(this.job.submitted_on);
      return date.toLocaleString();
    },
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
    }
    // add more methods as needed
  }
};
</script>
  
<style>
/* add more styles as needed */
</style>
  
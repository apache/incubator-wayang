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
    <div class="card rounded-0 p-0 m-1" style="width: 15rem;">
        <h5 class="card-header text-capitalize">{{ card_data.status }}</h5>
        <div class="card-body">
            <h6 class="card-title">Jobs: {{ card_data.jobCount }}</h6>
            <p class="card-text">Latest Submitted Job:</p>
            <router-link :to="'/jobs/' + card_data.latestJob" class="btn btn-outline-secondary btn-sm rounded-0">
                <span class="d-inline-block text-truncate" style="max-width: 100px;">{{card_data.latestJobName}}</span>
            </router-link>
        </div>
    </div>
</template>
  
<script>
export default {
    name: "JobSummaryCard",
    props: {
        card_data: {
            type: Object,
            required: true,
        },
    },
    computed: {
        formattedDuration() {
      let duration = this.card_data.avgRunningTime;
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
    }
};
</script>
  
<style scoped></style>
  
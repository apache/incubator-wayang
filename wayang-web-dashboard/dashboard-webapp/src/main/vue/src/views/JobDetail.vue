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
  <div class="container my-3 py-1">
    <div class="row">
      <div class="col-md-12">
        <div class="card rounded-0">
          <div class="card-header">Job Details</div>
          <div class="card-body">
            <JobDetails :job="job"></JobDetails>
          </div>
        </div>
      </div>
    </div>
    <div class="row py-3">
      <div class="col-md-12">
        <div class="col-md-12">
          <div class="card rounded-0">
            <div class="card-header">Job Metrics</div>
            <div class="card-body">
              <JobMetricsContainer></JobMetricsContainer>
            </div>
          </div>
        </div>
      </div>
    </div>
    <div class="row py-1">
      <div class="col-md-12">
        <div class="col-md-12">
          <div class="card rounded-0">
            <div class="card-header">Job Plan</div>
            <div class="card-body">
              <JobPlan :graph="job.graph" :task_selected="task_id" v-if="job.graph" @task-selected="task_id = $event">
              </JobPlan>
              <div class="alert alert-warning" role="alert" v-else>
                No job plan available
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    <div id="hackit-debugger" v-if="job.hackit">
      <div class="row mt-3 py-1">
        <div class="col-md-12">
          <div class="card rounded-0">
            <div class="card-header">
              Hackit Debugger
            </div>
            <div class="card-body">
              <HackitControls @next-tuple="nextTuple" />
            </div>
          </div>
        </div>
      </div>
      <div class="row mt-3 flex-grow-1">
        <div class="col-md-12">
          <div class="card rounded-0">
            <div class="card-header">Tuples</div>
            <div class="card-body">
              <HackitDebugger :hackitAction="hackitAction" :jobId="jobId" :taskId="task_id"  />
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import JobDetails from '@/components/JobDetails.vue'
import HackitDebugger from '@/components/HackitDebugger.vue'
import JobPlan from '@/components/JobPlan.vue'
import HackitControls from '@/components/HackitControls.vue'
import JobMetricsContainer from '@/components/JobMetricsContainer.vue'

export default {
  name: 'JobDetail',
  components: {
    JobDetails,
    HackitDebugger,
    JobPlan,
    HackitControls,
    JobMetricsContainer
  },
  data() {
    return {
      job: {},
      jobId: this.$route.params.id,
      task_id: null,
      isHackitEnabled: false,
      hackitAction: null,
      metrics: {},
    }
  },
  mounted() {
    this.getJob(this.jobId)
      .then((data) => (this.job = data))
  },
  methods: {
    async getJob(id) {
      const data = await fetch(`http://localhost:3000/jobs/${id}`).then((response) => response.json())
      return data
    },
    nextTuple() {
      console.log('Next Tuple button pressed');
      this.hackitAction = 'nextTuple'
    }
  }
}
</script>

<style scoped></style>

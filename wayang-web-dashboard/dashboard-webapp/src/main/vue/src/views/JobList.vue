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
    <div class="container">
        <div class="row">
            <div class="col-12 ">
                <JobSummaryContainer :jobSummary="jobSummary"></JobSummaryContainer>
            </div>
        </div>
        <div class="row">
            <div class="col-12">
                <div class="job-list">
                    <div class="d-flex justify-content-between align-items-center mb-3">
                        <div class="input-group ">
                            <input v-model="searchQuery" class="form-control rounded-0" type="search"
                                placeholder="Search by name or status" aria-label="Search" />
                            <button class="btn btn-outline-secondary rounded-0" type="button"
                                @click="clearFilter">Clear</button>
                        </div>
                        <select v-model="selectedStatus" class="form-select rounded-0">
                            <option value="">Filter by status</option>
                            <option v-for="status in statusOptions" :key="status">{{ status }}</option>
                        </select>
                    </div>
                </div>
            </div>
        </div>
        <div class="row">
            <div class="col-12">

                <div class="table-responsive">
                    <table class="table table-striped table-hover">
                        <thead>
                            <tr>
                                <th>Job Name</th>
                                <th>Start Time</th>
                                <th>Duration</th>
                                <th>End Time</th>
                                <th>Stages</th>
                                <th>Tasks</th>
                                <th>Status</th>
                                <th>Plugins</th>
                                <th>Details</th>
                            </tr>
                        </thead>
                        <tbody>
                            <JobListItem v-for="job in filteredJobs" :key="job.id" :job="job" />
                        </tbody>
                    </table>
                </div>
                <div v-if="!filteredJobs.length" class="col-12">No jobs found.</div>
            </div>
        </div>
    </div>
</template>
  
  
  
<script>
import JobListItem from "@/components/JobListItem.vue";
import JobSummaryContainer from '@/components/JobSummaryContainer.vue'

export default {
    name: "JobList",
    components: {
        JobListItem,
        JobSummaryContainer
    },
    data() {
        return {
            jobs: [],
            searchQuery: "",
            selectedStatus: "",
            timer: null,
        };
    },
    computed: {
        filteredJobs() {
            return this.jobs.filter((job) => {
                let nameMatch = job.name
                    .toLowerCase()
                    .includes(this.searchQuery.toLowerCase());
                let statusMatch = job.status
                    .toLowerCase()
                    .includes(this.selectedStatus.toLowerCase());
                return nameMatch && statusMatch;
            });
        },
        statusOptions() {
            let options = new Set();
            this.jobs.forEach((job) => {
                options.add(job.status);
            });
            return Array.from(options);
        },
        jobSummary() {
            const summary = []
            const statusSet = new Set(this.jobs.map(job => job.status))
            const statusList = Array.from(statusSet)
            for (const status of statusList) {
                const filteredJobs = this.jobs.filter(job => job.status === status)
                const jobCount = filteredJobs.length
                const avgRunningTime =
                    jobCount > 0
                        ? Math.round(
                            filteredJobs.reduce(
                                (total, job) => total + job.duration,
                                0
                            ) / jobCount
                        )
                        : 0
                const latestJob =
                    jobCount > 0
                        ? filteredJobs.reduce((prev, curr) =>
                            prev.submitted_on > curr.submitted_on ? prev : curr
                        ).id
                        : null
                const latestJobName = latestJob ? filteredJobs.filter(job => job.id === latestJob)[0].name : null
                summary.push({
                    status,
                    jobCount,
                    avgRunningTime,
                    latestJob,
                    latestJobName,
                })
            }
            return summary
        },
    },
    methods: {
        fetchJobs() {
            fetch("http://localhost:3000/jobs_overview")
                .then((response) => response.json())
                .then((data) => {
                    this.jobs = data;
                })
                .catch((error) => {
                    console.error("Error fetching jobs: ", error);
                });
        },
        startTimer() {
            this.timer = setInterval(() => {
                this.fetchJobs();
            }, 1000);
        },
        stopTimer() {
            clearInterval(this.timer);
        },
    },
    mounted() {
        this.fetchJobs();
        this.startTimer();
    },
    beforeUnmount() {
        this.stopTimer();
    },
};
</script>
  
<style></style>
  
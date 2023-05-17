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
    <div class="overview">
      <div class="container">
        <div class="row">
          <overview-card v-for="(item, index) in data" :key="index">
            <template #title>{{ item.title }}</template>
            <template #metric>{{ item.body }}</template>
          </overview-card>
        </div>
      </div>
    </div>
  </template>
  
  
  
  
<script>
import OverviewCard from '../components/OverviewCard.vue';

export default {
    components: {
        OverviewCard,
    },
    data() {
        return {
            data: [],
        };
    },
    methods: {
    },
    async created() {
        try {
            const response = await fetch('http://localhost:3000/overview');
            this.data = await response.json();
        } catch (error) {
            console.error('Error fetching data:', error);
        }
    },
};
</script>
  
<style scoped>

/* Add any other desired styles */
</style>
  
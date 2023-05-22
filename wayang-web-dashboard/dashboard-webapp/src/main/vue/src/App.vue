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
<template id="app">
  <div class="container-fluid bg-success-subtle">
    <div class="row ">
      <div class="col-12">
        <router-link to="/" class="navbar-brand">
          <img src="/logo.png" alt="Logo" class="logo" />
        </router-link>
      </div>
    </div>
    <div class="row p-3">
      <div class="col-2">
        <ul class="navbar-nav ms-auto p-3">
          <li class="nav-item btn rounded-0 border-0" :class="activeRoute === 'Overview' ? 'btn-secondary' : 'btn-outline-secondary'">
            <router-link to="/overview" class="nav-link">Wayang Overview</router-link>
          </li>
          <li class="nav-item btn rounded-0 border-0" :class="activeRoute === 'JobList' ? 'btn-secondary' : 'btn-outline-secondary'">
            <router-link to="/jobs" class="nav-link">Jobs</router-link>
          </li>
          <li class="nav-item btn btn-secondary rounded-0 border-0" v-if="activeRoute === 'JobDetail'">
            <span>{{$route.params.id}}</span>
          </li>
        </ul>
      </div>
      <div class="col-9">
        <router-view />
      </div>
    </div>
  </div>
</template>
<script>
export default{
  name: 'App',
  computed:{
    activeRoute(){
     return this.$route.name
    }
  }
}
</script>
<style scoped>
.logo {
  max-height: 50px;
}
</style>
  
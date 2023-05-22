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
    <div class="hackit-controls">
        <button class="btn rounded-0" :class="{ 'btn-primary': isRunning }" :disabled="!isRunning" @click="onPlay">
            Play
        </button>
        <button class="btn rounded-0" :class="{ 'btn-primary': isPaused }" :disabled="!isPaused" @click="onPause">
            Pause
        </button>
        <button class="btn rounded-0" :class="{ 'btn-primary': isVirtualPaused }" :disabled="!isVirtualPaused"
            @click="onVirtualPause">
            Virtual Pause
        </button>
        <button class="btn rounded-0" :class="{ 'btn-primary': isNextTupleActive }" :disabled="!isNextTupleActive"
            @click="onNextTuple">
            Next Tuple
        </button>
        <button class="btn rounded-0" :class="{ 'btn-primary': isNextOperatorActive }" :disabled="!isNextOperatorActive"
            @click="onNextOperator">
            Next Operator
        </button>
    </div>
</template>
  
<script>
export default {
    name: "HackitControls",
    data() {
        return {
            isRunning: false,
            isPaused: true,
            isVirtualPaused: true,
            isNextTupleActive: false,
            isNextOperatorActive: false,
        };
    },
    methods: {
        onPlay() {
            if (this.isRunning) {
                this.$emit("play", "isRunning");
                this.isRunning = false
                this.isPaused = true
                this.isVirtualPaused = true
                this.isNextOperatorActive = false
                this.isNextTupleActive = false
            }
        },
        onPause() {
            if (this.isPaused) {
                this.$emit("pause", "isPaused");
                this.isRunning = true
                this.isPaused = false
                this.isVirtualPaused = false
                this.isNextOperatorActive = true
                this.isNextTupleActive = true
            }
        },
        onVirtualPause() {
            if (this.isVirtualPaused) {
                this.$emit("virtual-pause", "isVirtualPaused");
                this.isRunning = true
                this.isVirtualPaused = false
                this.isNextOperatorActive = true
                this.isNextTupleActive = true
            }
        },
        onNextTuple() {
            if (this.isNextTupleActive) {
                this.$emit("next-tuple", "isNextTupleActive");
            }
        },
        onNextOperator() {
            if (this.isNextOperatorActive) {
                this.$emit("next-operator", "isNextOperatorActive");
            }
        },
    },
};
</script>
  
<style scoped></style>
  
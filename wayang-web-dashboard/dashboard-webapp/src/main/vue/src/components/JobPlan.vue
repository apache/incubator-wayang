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
  <div>
    <h3 class="my-3">Job Plan</h3>
    <div id="cy"></div>
  </div>
</template>

<script>
import cytoscape from 'cytoscape'

export default {
  name: 'JobPlan',
  props: {
    graph: Object,
    task_id: String
  },
  mounted() {
    const elements = {
      nodes: this.graph.nodes.map(node => ({ ...node, style: { 'background-color': '#666' } })),
      edges: this.graph.edges
    }

    const cy = cytoscape({
      container: document.getElementById('cy'),
      elements: elements,
      style: [
        {
          selector: 'node',
          style: {
            'background-color': '#666',
            'label': 'data(label)',
            'width': '20px',
            'height': '20px',
            'font-size': '10px',
            'text-valign': 'top',
            'text-halign': 'center',
            'text-margin-y': '-10px'
          }
        },
        {
          selector: 'edge',
          style: {
            'width': 3,
            'line-color': '#ccc',
            'target-arrow-color': '#ccc',
            'target-arrow-shape': 'triangle'
          }
        },
        {
          selector: '.selected',
          style: {
            'border-color': 'red',
            'border-width': '2px'
          }
        }
      ],
      layout: {
        name: 'grid'
      },
      userZoomingEnabled: false
    })

    // Add the 'selected' class to the corresponding node
    const selectNode = (task_id) => {
      const node = cy.nodes(`[task_id="${task_id}"]`);
      if (node.length > 0) {
        node.addClass('selected');
      }
    }

    cy.on('tap', 'node', (event) => {
      if (event.target.hasClass('selected')) {
        event.target.removeClass('selected');
        this.$emit('task-selected', null);
      } else {
        cy.nodes().removeClass('selected')
        event.target.addClass('selected')
        this.$emit('task-selected', event.target.data('task_id'))
      }
    })

    // Watch for updates to the 'task_id' prop and apply the 'selected' class to the corresponding node
    this.$watch('task_id', (newVal, oldVal) => {
      cy.nodes().removeClass('selected');
      if (newVal && newVal !== '') {
        selectNode(newVal);
      }
    });

    cy.fit()
  }
}
</script>

<style scoped>
#cy {
  height: 200px;
  width: 100%;
  margin: 0 auto;
  display: block;
}
</style>

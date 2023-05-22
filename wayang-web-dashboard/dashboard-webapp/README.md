# Hackit Web

This folder contains all the element to create the service "web"

## Initial test dashboard

The dashboard is initially developed with the Vue.js framework. A test data is provided with json-server.

Staring the demo database as follows:

```
npm install -g json-server
json-server --watch hackit-web/src/main/vue/src/assets/db.json
```

Then start the vue front-end server

```
cd hackit-web/src/main/vue
npm run dev
```

Vite will expose the web server and will indicate the port where is available. If no conflict with other applications, the default port is 5174.

With a browser access http://localhost:5175/
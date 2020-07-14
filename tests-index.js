var req = require.context('./src', true, /\.spec$/);
req.keys().forEach(req);

require('vscode-mocha-hmr')(module);

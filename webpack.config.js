const webpack = require('webpack');
const path = require('path');
const nodeExternals = require('webpack-node-externals');


module.exports = {
    entry: {
        // entry for your application
        // 'main': ['webpack/hot/poll?100', './src/main.ts'],
        // entry for your tests
        'tests': ['webpack/hot/poll?100', './tests-index.js'],
    },
    watch: true,
    target: 'node',
    devtool: 'source-map',
    mode: 'development',
    node: {
        // required if you want __dirname to behave as usual https://webpack.js.org/configuration/node/
        __dirname: false
    },

    // do not bundle node_modules diretory
    externals: [
        nodeExternals({
            whitelist: ['webpack/hot/poll?100'],
        }),
    ],

    // typescript loader
    module: {
        rules: [
            {
                test: /.tsx?$/,
                loader: 'ts-loader',
                options: {
                    transpileOnly: true,
                },
                exclude: /node_modules/,
            },
        ],
    },
    resolve: {
        extensions: ['.tsx', '.ts', '.js'],
        // this one is usually useful (not required for this example)
        // plugins: [new TsconfigPathsPlugin({ configFile: path.resolve(__dirname, 'tsconfig.json') })],
    },
    plugins: [
        // required
        new webpack.HotModuleReplacementPlugin(),
        // new webpack.SourceMapDevToolPlugin({
        //   // this will emit the right path in sourcemaps so mocha finds the right files
        //   publicPath: path.join('file:///', __dirname),
        //   fileContext: 'public',
        // })
    ],

    output: {
        path: path.join(__dirname, 'output'),
        // this ensures that source maps are mapped to actual files (not "webpack:" uris)
        devtoolModuleFilenameTemplate: info => path.resolve(__dirname, info.resourcePath),
    },
};
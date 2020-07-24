const webpack = require('webpack');
const path = require('path');
const nodeExternals = require('webpack-node-externals');

var isCoverage = process.env.NODE_ENV === 'coverage';

module.exports = {
    entry: {
        'tests': isCoverage
            ? ['./tests-index.js']
            : ['webpack/hot/poll?100', './tests-index.js']
    },
    watch: true,
    target: 'node',
    devtool: 'source-map',
    mode: 'development',
    node: {
        // required if you want __dirname to behave as usual https://webpack.js.org/configuration/node/
        __dirname: false
    },

    externals: [
        nodeExternals({
            whitelist: ['webpack/hot/poll?100'],
        }),
    ],

    module: {
        rules: [
            {
                test: /\.ts$/,
                loader: 'ts-loader',
                options: {
                    transpileOnly: true,
                },
            },
            ...isCoverage ? [
                {
                    test: /\.ts$/,
                    exclude: /\.spec\.ts$/,
                    enforce: 'post',
                    use: {
                        loader: 'istanbul-instrumenter-loader',
                        options: { esModules: true }
                    }
                }] : [],
            // {
            //     test: /\.pegjs$/,
            //     loader: 'pegjs-loader'
            // },
            {
                test: /\.ne$/,
                use: [
                    {
                        loader: path.resolve(__dirname, 'nearley-loader.js')
                    }
                ],
            },
        ],
    },
    resolve: {
        extensions: ['.tsx', '.ts', '.js', '.ne'], // '.pegjs',
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
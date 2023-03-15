const webpack = require('webpack');
const path = require('path');
const nodeExternals = require('webpack-node-externals');
const CopyPlugin = require('copy-webpack-plugin');
const resolve = file => path.resolve(__dirname, '..', file);

var isCoverage = process.env.NODE_ENV === 'coverage';
const mode = process.argv.includes('--prod')
    ? 'production'
    : 'development';

module.exports = {
    entry: mode === 'production' ? {
        'index': resolve('src/index.ts'),
    } : {
        'tests': isCoverage
            ? [resolve('tools/tests-index.js')]
            : ['webpack/hot/poll?100', resolve('tools/tests-index.js')]
    },
    watch: mode === 'development',
    optimization: {
        minimize: false,
    },
    target: 'node',
    devtool: 'source-map',
    mode,

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
                    transpileOnly: mode === 'development',
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
        ],
    },
    resolve: {
        extensions: ['.tsx', '.ts', '.js', '.ne'],
    },
    plugins: mode === 'production' ? [
        new CopyPlugin({
            patterns: [
                { from: 'package.json', to: 'package.json' },
                { from: 'readme.md', to: 'readme.md' },
            ],
        }),
    ]
        : [
            // required
            new webpack.HotModuleReplacementPlugin(),
        ],

    output: {
        library: '',
        libraryTarget: 'commonjs',
        path: mode === 'production'
            ? resolve('lib')
            : resolve('output'),
        // this ensures that source maps are mapped to actual files (not "webpack:" uris)
        devtoolModuleFilenameTemplate: mode === 'production'
            ? info => info.resourcePath
            : info => resolve(info.resourcePath),
    },
};
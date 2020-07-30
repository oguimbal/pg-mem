const webpack = require('webpack');
const path = require('path');
const nodeExternals = require('webpack-node-externals');
const CopyPlugin = require('copy-webpack-plugin');

var isCoverage = process.env.NODE_ENV === 'coverage';
const mode = process.argv.includes('--prod')
    ? 'production'
    : 'development';

module.exports = {
    entry: mode === 'production' ? {
        'index': './src/index.ts',
    } : {
            'tests': isCoverage
                ? ['./tests-index.js']
                : ['webpack/hot/poll?100', './tests-index.js']
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
            ? path.join(__dirname, 'lib')
            : path.join(__dirname, 'output'),
        // this ensures that source maps are mapped to actual files (not "webpack:" uris)
        devtoolModuleFilenameTemplate: info => info.resourcePath,
    },
};
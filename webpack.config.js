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
    target: 'node',
    devtool: 'source-map',
    mode,
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
        extensions: ['.tsx', '.ts', '.js', '.ne'], // '.pegjs',
        // this one is usually useful (not required for this example)
        // plugins: [new TsconfigPathsPlugin({ configFile: path.resolve(__dirname, 'tsconfig.json') })],
    },
    plugins: mode === 'production' ? [
        new CopyPlugin({
            patterns: [
                { from: 'package.json', to: 'package.json' },
                { from: 'readme.md', to: 'readme.md' },
                { from: 'index.d.ts', to: 'index.d.ts' },
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
        devtoolModuleFilenameTemplate: info => path.resolve(__dirname, info.resourcePath),
    },
};
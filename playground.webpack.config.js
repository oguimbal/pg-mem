const webpack = require('webpack');
const path = require('path');
const MonacoWebpackPlugin = require('monaco-editor-webpack-plugin');
const APP_DIR = path.resolve(__dirname, './playground');
const MONACO_DIR = path.resolve(__dirname, './node_modules/monaco-editor');
const HtmlWebPackPlugin = require('html-webpack-plugin');
const libConfig = require('./webpack.config.js');
const CopyPlugin = require('copy-webpack-plugin');

const mode = process.argv.includes('--prod')
    ? 'production'
    : 'development';
process.env.NODE_ENV = mode;
const ouptutpath = path.resolve(__dirname, 'dist');
require('rimraf').sync(ouptutpath);

const common = {
    mode,
    devtool: 'source-map',
    optimization: {
        minimize: true,
        namedModules: true,
        concatenateModules: true,
    },
    plugins: [
        new MonacoWebpackPlugin({
            // available options are documented at https://github.com/Microsoft/monaco-editor-webpack-plugin#options
            languages: ['pgsql']
        }),
    ],
    module: {
        rules: [
            ...libConfig.module.rules,
            {
                test: /\.tsx$/,
                loader: 'ts-loader',
                options: {
                    transpileOnly: true,
                },
            }, {
                test: /\.(js|mjs)$/,
                include: path.resolve(__dirname, 'node_modules/react-data-grid'),
                exclude: /@babel(?:\/|\\{1,2})runtime/,
                loader: require.resolve('babel-loader'),
                options: {
                    babelrc: false,
                    configFile: false,
                    compact: false,
                    // this plugins section:
                    plugins: [
                        require.resolve("@babel/plugin-proposal-nullish-coalescing-operator"),
                        require.resolve("@babel/plugin-proposal-optional-chaining"),
                    ],
                    presets: [
                        [
                            require.resolve('babel-preset-react-app/dependencies'),
                            { helpers: true },
                        ],
                    ],
                }
            }, {
                test: /\.css$/,
                include: APP_DIR,
                use: [{
                    loader: 'style-loader',
                }, {
                    loader: 'css-loader',
                    options: {
                        modules: true,
                        namedExport: true,
                    },
                }],
            }, {
                test: /\.css$/,
                include: MONACO_DIR,
                use: ['style-loader', 'css-loader'],
            },
            {
                test: /\.css$/i,
                exclude: MONACO_DIR,
                use: ['style-loader', 'css-loader'],
                //   use: [MiniCssExtractPlugin.loader, 'css-loader'],
            },
            {
                test: /\.(eot|otf|ttf|woff|woff2)$/,
                use: 'file-loader',
            },

        ],
    },
};
module.exports = [
    {
        ...common,
        resolve: {
            extensions: ['.css', ...libConfig.resolve.extensions],
            alias: {
                'react-dom': '@hot-loader/react-dom',
            },
        },
        devServer: {
            contentBase: path.join(__dirname, 'dist'),
            compress: true,
            port: 9000
        },
        plugins: [
            new HtmlWebPackPlugin({
                template: path.resolve(__dirname, 'playground/index.html'),
                filename: 'index.html'
            }),
            new CopyPlugin({
                patterns: [
                    { from: 'playground/index.css', to: 'index.css' },
                ],
            }),
            new MonacoWebpackPlugin({
                // available options are documented at https://github.com/Microsoft/monaco-editor-webpack-plugin#options
                languages: ['pgsql']
            }),
            new webpack.EnvironmentPlugin({ NODE_ENV: 'development' }),
        ],
        entry: {
            playground: ['react-hot-loader/patch', './playground/index.tsx'],
        },
        output: {
            path: ouptutpath,
            filename: 'main.js',
        },
        optimization: {
            runtimeChunk: 'single',
            splitChunks: {
                chunks: 'all',
                maxInitialRequests: Infinity,
                minSize: 0,
                cacheGroups: {
                    vendor: {
                        test: /[\\/]node_modules[\\/]/,
                        name(module) {
                            // get the name. E.g. node_modules/packageName/not/this/part.js
                            // or node_modules/packageName
                            const packageName = module.context.match(/[\\/]node_modules[\\/](.*?)([\\/]|$)/)[1];

                            // npm package names are URL-safe, but some servers don't like @ symbols
                            return `npm.${packageName.replace('@', '')}`;
                        },
                    },
                },
            },
        },
    },
];
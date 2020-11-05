const fs = require('fs');
const path = require('path');
var nearley = require('nearley/lib/nearley.js');
var compile = require('nearley/lib/compile.js');
var generate = require('nearley/lib/generate.js');
var lint = require('nearley/lib/lint');
var rawGrammar = require('nearley/lib/nearley-language-bootstrapped.js');

var nearleyGrammar = nearley.Grammar.fromCompiled(rawGrammar);

module.exports = function (input) {
  var parser = new nearley.Parser(nearleyGrammar);
  console.log('Parsing nearley', this.resource);
  parser.feed(input);
  const opts = {
    args: [this.resourcePath],
    alreadycompiled: [],
  }
  var compilation = compile(parser.results[0], opts);
  if (this.addDependency) {
    for (const dep of opts.alreadycompiled) {
      this.addDependency(dep);
    }
  }
  lint(compilation, {});
  const ret = generate(compilation, 'grammar');
  // fs.writeFileSync(this.resource + '.compiled.ts', ret);
  return ret;
}
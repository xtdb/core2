const fs = require('fs');

module.exports = function (registry) {
  registry.postprocessor('railroad', function () {
    console.log('postprocessor');
    var self = this
    self.process(function (doc, output) {
      var proc = doc.attributes.$$smap.docfile;
      console.log('processing:',proc);

      // TODO: make this match multiple railroad:: blocks
      var matcher = /railroad::(?<filename>.*)\[\]/;
      var svg = output.match(matcher);

      if (svg != null) {
        console.log('svg is:',svg.groups.filename);
        var path = './railroads/'.concat(svg.groups.filename);
        var contents;

        try {
          contents = fs.readFileSync(path, 'utf8');
          console.log(contents);
        } catch (err) {
          console.error(err);
        }

        return output.replace(matcher, contents);
      } else {
        return output;
      }
    })
  })
}

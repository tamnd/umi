The first body opens here, and it holds the part of the page that the outer template was responsible for producing before it handed over.

A second body tag, emitted by an include that did not know it was being included into a document that already had one. A browser folds these together and so does a conformant parser, which is the reason doc 11.3 insists on one.

A third body, this time after the closing html tag, which is the shape you get when a caching layer appends a footer fragment to a response it did not parse.
# Every link on this page is relative

The base tag moves the whole document to a different origin as far as relative links are concerned, which is a thing static site generators do when they push assets to a CDN and forget that the HTML went with them.

- [A sibling page](https://cdn.corpus.example/v2/docs/install)
- [One level up](https://cdn.corpus.example/v2/index.html)
- [An absolute path, which ignores the directory](https://cdn.corpus.example/absolute/path)
- [An absolute URL, which ignores all of it](https://elsewhere.example/page)
- A fragment, which points at this page and is dropped
- [An email address, which is content](mailto:harbour@corpus.example)

[The logo](https://cdn.corpus.example/v2/docs/img/logo.png) is relative too.
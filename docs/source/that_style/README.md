# that style
A plain, more modern HTML style for Doxygen

The style makes Doxygen documentation look like this:
![Screenshot of class doc](/screenshots/screenshot_class.png?raw=true "Screenshot of a class documentation")

![Screenshot of function doc](/screenshots/screenshot_function.png?raw=true "Screenshot of a function documentation")

The full documentation these screenshots are taken from can be found here: https://jl-wynen.github.io/isle/index.html

## Requirements
- Doxygen >= 1.8.13 (tested with version 1.8.13 and 1.8.14)
- *optional*: a sass/scss compiler if you want to modify the style

## Simple usage
Tell Doxygen about the files for that style as shown in [doxyfile.conf](doxyfile.conf). You might need to adjust the
paths depending on where you installed that style.

When you run Doxygen, all files are copied into the generated HTML folder. So you don't need to keep the originals around
unless you want to re-generate the documentation.

## Advanced
that style uses a custom javascript to hack some nice stripes into some tables. It has to be loaded from HTML. Hence you need
to use the provided custom header. Since its default content may change when Doxygen is updated, there might be syntax errors in
the generated HTML. If this is the case, you can remove the custom header (adjust your doxyfile.conf). This has no
disadvantages other than removing the stripes.

[that_style.css](that_style.css) was generated from the scss files in the folder [sass](sass). If you want to change the style,
use those files in order to have better control. For instance, you can easily change most colors by modifying the variables
in the beginning of [that_style.scss](sass/that_style.scss).

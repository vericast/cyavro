cyavro
======

<table>
<tr>
  <td>Latest Release</td>
  <td>
    <a href="https://pypi.python.org/pypi/cyavro">
      <img src="https://img.shields.io/pypi/v/cyavro.svg" alt="latest release" />
    </a>
  </td>
</tr>
<tr>
  <td>License</td>
  <td>
    <a href="https://github.com/maxpoint/cyavro/blob/master/LICENSE.txt">
    <img src="https://anaconda.org/mvn/cyavro/badges/license.svg" alt="cyavro license" />
    </a>
  </td>
</tr>
<tr>
  <td>Build Status</td>
  <td>
    <a href="https://travis-ci.org/maxpoint/cyavro">
    <img src="https://travis-ci.org/maxpoint/cyavro.svg" alt="build status" />
    </a>
  </td>
</tr>
<tr>
  <td>PyPI</td>
  <td>
    <a href="https://pypi.python.org/pypi/cyavro/">
    <img src="https://img.shields.io/pypi/dm/cyavro.svg" alt="pypi downloads" />
    </a>
  </td>
</tr>
<tr>
  <td>Documentation</td>
  <td>
    <a href="https://maxpoint.github.io/cyavro/">
    maxpoint.github.io/cyavro/
    </a>
  </td>
</tr>
</table>

This package provides a substantial speed improvement when reading and writing avro files over the
pure python implementation.


Installation
------------
Installing cyavro requires several c libraries to be present.  The simplest way to build and install cyavro
is by using the conda recipes provided.  Building these should work on linux and mac.

Windows is unsupported.

The simlest way to install is via conda

```bash
  conda install -c conda-forge cyavro
```

Building
--------

```bash
  cd conda-recipes
  conda build cyavro
```

Simple Usage
------------

```python
  import cyavro
  cyavro.read_avro_file_as_dataframe("/path/to/somefile.avro")
```




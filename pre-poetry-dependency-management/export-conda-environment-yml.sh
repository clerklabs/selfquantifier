conda env export -p $1 | grep -v 'prefix: ' | grep -v 'name: '

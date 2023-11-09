Arguably "mail" is already plural so naming this package "lib.email" would be
more natural, and python understands the imports. However, pylance will randomly
freak out because there is a built-in module called "email" and for some reason
that confuses it (despite there being no ambiguity in practice). Hence I renamed
this to emails

# This is an example test settings file for use with the Django test suite.
#
# The 'sqlite3' backend requires only the ENGINE setting (an in-
# memory database will be used). All other backends will require a
# NAME and potentially authentication information. See the
# following section in the docs for more information:
#
# https://docs.djangoproject.com/en/dev/internals/contributing/writing-code/unit-tests/
#
# The different databases that Django supports behave differently in certain
# situations, so it is recommended to run the test suite against as many
# database backends as possible.  You may want to create a separate settings
# file for each of the backends you test against.

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'django_testdb_default',
        'USER': 'root',
        'PASSWORD': 'kakk0la',
        'SCHEMA': 'testing_schema',
        'HOST': '',
        'PORT': '',
        'OPTIONS': {
               'init_command': 'SET storage_engine=INNODB',
        }
    },
    'other': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'django_testdb_other',
        'USER': 'root',
        'PASSWORD': 'kakk0la',
        'HOST': '',
        'PORT': '',
        'OPTIONS': {
               'init_command': 'SET storage_engine=INNODB',
        }
    },
}
SECRET_KEY = 'a'

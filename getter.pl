#!/usr/bin/env perl

use strict;
use warnings;

our $DSN = 'DBI:mysql:database=gec';
our $USER = 'cdent';
our $GEC;

use GEC;
$GEC = GEC->new(ename => 'hl7', dsn => $DSN, user => $USER);

use YAML;

my $data = $GEC->get($ARGV[0]);

print Dump($data);

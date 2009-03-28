#!/usr/bin/env perl

use strict;
use warnings;

our $DSN = 'DBI:mysql:database=gec';
our $USER = 'cdent';
our $GEC;

use GEC;
$GEC = GEC->new(ename => 'hl7', dsn => $DSN, user => $USER);

use YAML;

foreach my $id (@ARGV) {
    my $data = $GEC->get($id);
    print Dump($data);
}

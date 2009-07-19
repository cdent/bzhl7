#!/usr/bin/env perl

use strict;
use warnings;

our $USER = 'cdent';
our $GEC;

use GEC;
our $DSN = GEC->ReadDSN();
$GEC = GEC->new(ename => 'hl7', dsn => $DSN, user => $USER);

use YAML;

foreach my $id (@ARGV) {
    my $data = $GEC->get($id);
    print Dump($data);
}

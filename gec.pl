#!/usr/bin/perl
use strict;
use warnings;

use Data::UUID;
use YAML;
use GEC;

my $dsn = 'DBI:mysql:database=gec';
my $user = 'cdent';

my $gec = GEC->new(ename => 'test', dsn => $dsn, user => $user);
$gec->create();
my $hash = {
    foo => 'artificial canaries',
    bar => 'collapsed arterty',
    muscles => 'blue',
    heart => 'black',
};

my $id = Data::UUID->new->create_str();
$id = $gec->put($hash, $id);
my $out = $gec->get($id);
warn Dump($out);

warn Dump($gec->all());

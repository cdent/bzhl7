#!/usr/bin/env perl

use strict;
use warnings;

my $pid;
my $post_pid;
my $obrid;
my $name;
my $obx;
my $obr;

# This loop only works if and only if
# 
# The file has been processed with s/\r/\n/ (dos2unix)
#
# AND
#
# Each record:
# # Starts with MSH
# # Ends with an empty line (including the final record)
# # Has a line begining with PID that contains record
# # # information (such as name of patient).
# # Has multiple lines that begin with OBX.
#
# This doesn't yet attend to parsing the OBX data.

# run like this (once the data file has been dos2unix
# process has been done):
#
#    ./slice_dice.pl < <data_file>
#
while (<>) {
    s/\s+$//; # a simple chomp doesn't work here
    /^MSH/ && do {
        # new record, destroy exiting data
        undef $pid;
        undef $post_pid;
        undef $obrid;
        undef $obr;
        undef $name;
        $obx = [];
    };
    /^PID/ && do {
        # get the pid line 
        my @splits = split('\|', $_);
        $pid = $splits[3];
        $post_pid = $splits[-1];
        $name = $splits[5];
        $name =~ s/\^/ /g;
    };
    /^OBR/ && do {
        my @splits = split('\|', $_);
        $obrid = $splits[2];
        $obr = $splits[21];
    };
    /^OBX/ && do {
        my @splits = split('\|', $_);
        push(@$obx, $splits[5]);
    };
    /^$/ && do {
        # we now have a complete record
        # in the future this will write files 
        # or whatever
        no strict 'refs';
        &$obr(pid => $pid, name => $name, post_pid => $post_pid, obrid => $obrid, obx => $obx, obr => $obr);
        #exit;
    };
}

our $AUTOLOAD;
sub AUTOLOAD {
    $AUTOLOAD =~ s/.*://;
    return if $AUTOLOAD eq 'DESTROY';
    #warn "no method for $AUTOLOAD";
}


sub MCTH {
    my %inparams = @_;
    my %params = (
        # defaults
        %inparams,
    );
    print "filename: ", $params{pid}, '_', $params{post_pid}, '_', $params{obrid}, '.html', "\n";
    print "name: $params{name}\n";
    print "obr: $params{obr}\n";
    foreach my $obx (@{$params{obx}}) {
        if ($obx and $obx =~ /\s*([[:upper:] ]+):$/) {
            my $section = $1;
            print "$section\n";
        } else {
            #print "\n";
        }
    }
}

sub EMCTH {
    return MCTH($@);
}

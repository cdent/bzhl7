use strict;
use warnings;
use Time::Local;

my $timestamp = $ARGV[0];

my ($year, $month, $day, $hour, $min, $sec) =
    ($timestamp =~ /(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})/);

my $now = time;
my $then = timegm($sec, $min, $hour, $day, $month-1, $year);

my $gap = $now - $then;
my $gap_days = $gap/(24 * 60 * 60);

print $gap_days, " before now\n";


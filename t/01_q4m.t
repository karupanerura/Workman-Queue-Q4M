use strict;
use warnings;
use utf8;
use Test::More;

use t::Util;
use Workman::Queue::Q4M;
use Workman::Test::Queue;

my @connect_info = t::Util->setup(qw/Foo Bar/);
END {
    t::Util->cleanup(\@connect_info => [qw/Foo Bar/]);
}

my $queue = Workman::Queue::Q4M->new(connect_info => \@connect_info);
my $test  = Workman::Test::Queue->new($queue);
# $test->verbose(1);
$test->run;

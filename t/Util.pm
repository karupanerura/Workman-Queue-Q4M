package t::Util;
use strict;
use warnings;
use utf8;

use parent qw/Test::Builder::Module/;

use DBIx::Sunny;

# copied and arranged from Queue::Q4M
sub setup {
    my ($class, @tables) = @_;

    my $dsn      = $ENV{Q4M_DSN} || 'dbi:mysql:dbname=test';
    my $username = $ENV{Q4M_USER};
    my $password = $ENV{Q4M_PASSWORD};

    if ($dsn !~ /^dbi:mysql:/i) {
        $dsn = "dbi:mysql:database=$dsn";
    }

    my @connect_info;
    eval {
        @connect_info = (
            $dsn,
            $username,
            $password,
            { RaiseError => 1, AutoCommit => 1 }
        );

        my $dbh = DBIx::Sunny->connect(@connect_info);
        for my $table (@tables) {
            $dbh->do(<<"            EOSQL");
            CREATE TABLE IF NOT EXISTS $table (
                id INTEGER NOT NULL
            ) ENGINE=QUEUE;
            EOSQL
        }
    };
    if ($@) {
        __PACKAGE__->builder->diag($@);
        __PACKAGE__->builder->plan(skip_all => "Could not setup mysql");
    }

    return @connect_info;
}

sub cleanup {
    my ($class, $connect_info, $tables) = @_;
    my $dbh = DBI->connect(@$connect_info);
    for my $table (@$tables) {
        $dbh->do("DROP TABLE $table");
    }
}

1;

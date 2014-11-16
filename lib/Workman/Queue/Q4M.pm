package Workman::Queue::Q4M;
use strict;
use warnings;
use utf8;

our $VERSION = '0.01';

use parent qw/Workman::Queue/;
use Class::Accessor::Lite
    ro => [qw/connect_info timeout/],
    rw => [qw/task_names/];

use DBIx::Sunny;
use SQL::Maker;
use Workman::Job;
use Workman::Request;

use constant DEFAULT_TIMEOUT => 10;

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new(@_);
    $self->{timeout} ||= DEFAULT_TIMEOUT;
    return $self;
}

sub can_wait_job { 0 }

sub dbh {
    my $self = shift;
    delete $self->{dbh} if exists $self->{owner_pid} && $self->{owner_pid} != $$;
    return $self->{dbh} if exists $self->{dbh};

    $self->{owner_pid} = $$;
    return $self->{dbh} = DBIx::Sunny->connect(@{ $self->connect_info });
}

sub _sql_maker {
    my $self = shift;
    return $self->{_sql_maker} ||= SQL::Maker->new(driver => 'mysql');
}

sub register_tasks {
    my ($self, $task_set) = @_;
    $self->task_names([ $task_set->get_all_task_names ]);
}

sub enqueue {
    my ($self, $name, $args) = @_;
    my ($sql, @bind) = $self->_sql_maker->insert($name, $args);
    $self->dbh->query($sql, @bind);
    return Workman::Request->new(
        on_wait => sub {
            warn "[$$] Q4M hasn't support to wait result.";
            return;
        },
    );
}

sub dequeue {
    my $self = shift;

    my $index = do {
        my $args = [@{ $self->task_names }];
        push @$args => $self->timeout if defined $self->timeout;

        local $self->dbh->{private_in_queue_wait} = 1;
        $self->dbh->select_one('SELECT queue_wait(?)', $args);
    } or return;

    my $name = $self->task_names->[$index - 1];
    my $sql  = sprintf 'SELECT * FROM `%s`', $name;
    my $args = $self->dbh->select_row($sql);
    return Workman::Job->new(
        name    => $name,
        args    => $args,
        on_done => sub {
            my $result = shift;
            warn "[$$] Q4M hasn't support to send result." if defined $result;
            $self->dbh->select_one('SELECT queue_end()');
        },
        on_fail => sub {
            $self->dbh->select_one('SELECT queue_end()');
        },
        on_abort => sub {
            $self->dbh->select_one('SELECT queue_abort()');
        },
    );
}

sub dequeue_abort {
    my $self = shift;

    my $sth = $DBI::lasth;
    if ($sth && $sth->{Database}{private_in_queue_wait}) {
        die "[$$] RECEIVED TERM SIGNAL into queue_wait()";
    }
}

1;
__END__

=pod

=encoding utf-8

=head1 NAME

Workman::Queue::Q4M - queue manager for Workman

=head1 SYNOPSIS

    use Workman::Queue::Q4M;
    my $queue = Workman::Queue::Q4M->new(connect_info => [
        'dbi:mysql:dbname=mydb',
        $username,
        $password
    ]);
    my $profile = Workman::Server::Profile->new(max_workers => 10, queue => $queue);
    $profile->set_task_loader(sub {
        my $set = shift;

        warn "[$$] register tasks...";
        my $task = Workman::Task->new(Echo => sub {
            my $args = shift;

            ...;

            return;
        });
        $set->add($task);
    });

    # start
    Workman::Server->new(profile => $profile)->run();


=head1 DESCRIPTION

TODO

=head1 SEE ALSO

L<perl>

=head1 LICENSE

Copyright (C) karupanerura.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

karupanerura E<lt>karupa@cpan.orgE<gt>

=cut

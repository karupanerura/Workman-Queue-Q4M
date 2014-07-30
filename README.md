# NAME

Workman::Queue::Q4M - queue manager for Workman

# SYNOPSIS

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

# DESCRIPTION

TODO

# SEE ALSO

[perl](https://metacpan.org/pod/perl)

# LICENSE

Copyright (C) karupanerura.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# AUTHOR

karupanerura <karupa@cpan.org>

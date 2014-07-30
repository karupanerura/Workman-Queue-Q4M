requires 'Class::Accessor::Lite';
requires 'DBIx::Sunny';
requires 'SQL::Maker';
requires 'Workman::Job';
requires 'Workman::Queue';
requires 'Workman::Request';
requires 'parent';

on configure => sub {
    requires 'Module::Build::Tiny', '0.035';
    requires 'perl', '5.008_001';
};

on test => sub {
    requires 'DBI';
    requires 'Test::Builder::Module';
    requires 'Test::More';
    requires 'Workman::Test::Queue';
};

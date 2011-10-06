package IO::Storm::Bolt;
use Moose;

extends 'Storm';

use Log::Log4perl;

my $logger = Log::Log4perl->get_logger('storm.bolt');

sub process {
    my ($self, $tuple) = @_;
}

sub run {
    my ($self) = @_;

    my ($conf, $context) = $self->initbolt;
    while(1) {
        my $tup = $self->readtuple;
        $self->process($tup);
        $self->sync;
    }
}

1;
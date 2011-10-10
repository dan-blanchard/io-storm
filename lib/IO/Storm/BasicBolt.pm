package IO::Storm::BasicBolt;
use Moose;

extends 'Storm';

use Log::Log4perl;

my $logger = Log::Log4perl->get_logger('storm.basicbolt');

sub process {
    my ($self, $tuple) = @_;
}
    
sub run {
    my ($self) = @_;
    # XXX

    my ($conf, $context) = $self->init_bolt;
    while(1) {
        my $tup = $self->read_tuple;
        $self->_anchor($tup);
        $self->process($tup);
        $self->ack($tup);
        $self->sync;
    }
}

1;
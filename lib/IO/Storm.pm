package IO::Storm;
use Moose;

use IO::Handle qw(autoflush);
use IO::File;
use JSON::XS qw(decode_json encode_json);
use Log::Log4perl;
use Storm::Tuple;

# ABSTRACT: Perl support for Twitter's Storm

my $logger = Log::Log4perl->get_logger('storm');

=head1 DESCRIPTION

IO::Storm allows you to leverage Storm's multilang support to write Bolts
(and someday, more) in Perl.

=head1 SYNOPSIS

    package SplitSentenceBolt;
    use Moose;

    extends 'Storm::BasicBolt';

    sub process {
        my ($self, $tuple) = @_;

        my @words = split(' ', $tuple->values->[0]);
        foreach my $word (@words) {

            $self->emit([ $word ]);
        }
    }

    SplitSentenceBolt->new->run;

=cut

has '_stdin' => (
    is => 'rw',
    default => sub {
        my $io = IO::Handle->new;
        $io->fdopen(fileno(STDIN), 'r');
    }
);

sub read_string_message {
    my ($self) = @_;

    my @messages = ();
    while(1) {
        $logger->debug("reading");
        my $line = $self->_stdin->getline;
        chomp($line);
        $logger->debug("got $line");
        if($line eq 'end') {
            last;
        }
        push(@messages, $line);
    }
    return join("\n", @messages);
}

sub read_message {
    my ($self) = @_;

    return decode_json($self->read_string_message);
}

sub send_message_to_parent {
    my ($self, $href) = @_;

    $self->send_to_parent(encode_json($href));
}

sub send_to_parent {
    my ($self, $s) = @_;

    $logger->debug("sending $s");
    print "$s\n";;
    $logger->debug("sending end");
    print "end\n";
}

sub sync {
    my ($self) = @_;
    $logger->debug("sending sync");
    print "sync\n";
}

sub send_pid {
    my ($self, $hbdir) = @_;

    my $pid = $$;
    print "$pid\n";
    $logger->debug("sent $pid");
    
    # XXX error handling
    my $fh = IO::File->new;
    $fh->open('> '.$hbdir.'/'.$pid);
    $fh->close;
    
    $logger->debug("### wrote pid to $hbdir/$pid");    
}

sub emit_tuple {
    my ($self, $tuple, $stream, $anchors, $direct_task) = @_;
    
    # global ANCHOR_TUPLE
    # if ANCHOR_TUPLE is not None:
    #     anchors = [ANCHOR_TUPLE]
    # m = {"command": "emit"}
    # if stream is not None:
    #     m["stream"] = stream
    # m["anchors"] = map(lambda a: a.id, anchors)
    # if directTask is not None:
    #     m["task"] = directTask
    # m["tuple"] = tup
    # sendMsgToParent(m)

    my %message = ( command => 'emit' );
    if(defined($stream)) {
        $message{stream} = $stream;
    }
    if(defined($direct_task)) {
        $message{task} = $direct_task;
    }
    $message{tuple} = $tuple;
    $self->send_message_to_parent(\%message);
}

sub emit {
    my ($self, $tuple, $stream, $anchors) = @_;
    
    $anchors = [];
    
    $self->emit_tuple($tuple, $stream, $anchors);
    return $self->read_message;
}

sub emitDirect {
    # XXX
}

sub ack {
    my ($self, $tuple) = @_;
    
    $self->send_message_to_parent({ command => 'ack', id => $tuple->id })
}

sub fail {
    my ($self, $tuple) = @_;
    
    $self->send_message_to_parent({ command => 'fail', id => $tuple->id })
}

sub log {
    my ($self, $message) = @_;
    
    $self->send_message_to_parent({ command => 'log', msg => $message })
}

sub read_env {
    my ($self) = @_;
    $logger->debug("### read_env");
    my $conf = $self->read_message;
    my $context = $self->read_message;
    
    return [ $conf, $context ];
}

sub read_tuple {
    my ($self) = @_;

    my $tupmap = $self->read_message;

    return Storm::Tuple->new(
        id      => $tupmap->{id},
        component => $tupmap->{comp},
        stream  => $tupmap->{stream},
        task    => $tupmap->{task},
        values  => $tupmap->{tuple}
    );
}

sub init_bolt {
    my ($self) = @_;
    
    autoflush STDOUT 1;
    
    $logger->debug("init_bolt");
    my $hbdir = $self->read_string_message;
    $self->send_pid($hbdir);
    return $self->read_env;
}

1;

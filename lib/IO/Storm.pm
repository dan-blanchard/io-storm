# ABSTRACT: IO::Storm allows you to write Bolts and Spouts for Storm in Perl.

package IO::Storm;

# Imports
use strict;
use warnings;
use v5.10;
use IO::Handle qw(autoflush);
use IO::File;
use Log::Log4perl qw(:easy);
use JSON::XS;

use IO::Storm::Tuple;

# Setup Moo for object-oriented niceties
use Moo;
use namespace::clean;

has '_pending_commands' => (
    is      => 'rw',
    default => sub { [] },
);

has '_pending_taskids' => (
    is      => 'rw',
    default => sub { [] },
);

has '_stdin' => (
    is      => 'rw',
    default => sub {
        my $io = IO::Handle->new;
        $io->fdopen( fileno(STDIN), 'r' );
    }
);

has 'max_lines' => (
    is      => 'rw',
    default => 100
);

has 'max_blank_msgs' => (
    is      => 'rw',
    default => 500
);

has '_json' => (
    is      => 'rw',
    default => sub { JSON::XS->new->allow_blessed->convert_blessed }
);

my $logger = Log::Log4perl->get_logger('storm');

=method read_message

Read a message from the ShellBolt.  Reads until it finds a "end" line.

=cut

sub read_message {
    $logger->debug('start read_message');
    my $self         = shift;
    my $lines        = 0;
    my $blank_lines  = 0;
    my $message_size = 0;

    my @messages = ();
    while (1) {
        my $line = $self->_stdin->getline;
        $logger->debug( 'read_message: line=' . $line );
        chomp($line);
        if ( $line eq 'end' ) {
            last;
        }
        $lines++;

        # If Storm starts to send us a series of blank lines, after awhile
        # we have to assume that the pipe to the Storm supervisor is broken
        if ( $line eq '' ) {
            $blank_lines++;
            if ( $blank_lines >= $self->max_blank_msgs ) {
                die( "$blank_lines blank lines received, assuming pipe to "
                        . "Storm supervisor is broken" );
            }
        }
        elsif ( $lines >= $self->max_lines ) {
            die "Message exceeds "
                . $self->max_lines
                . " lines, assuming this is an error.";
        }

        push( @messages, $line );
    }

    return $self->_json->decode( join( "\n", @messages ) );
}

sub read_task_ids {
    my $self = shift;

    if ( @{ $self->_pending_taskids } ) {
        return shift( $self->_pending_taskids );
    }
    else {
        my $msg = $self->read_message();
        while ( ref($msg) ne 'ARRAY' ) {
            push( $self->_pending_commands, $msg );
            $msg = $self->read_message();
        }

        return $msg;
    }
}

sub read_command {
    my $self = shift;

    if ( @{ $self->_pending_commands } ) {
        return shift( $self->_pending_commands );
    }
    else {
        my $msg = $self->read_message();
        while ( ref($msg) eq 'ARRAY' ) {
            push( $self->_pending_taskids, $msg );
            $msg = $self->read_message();
        }
        return $msg;
    }
}

=method read_tuple

Turn the incoming Tuple structure into an <IO::Storm::Tuple>.

=cut

sub read_tuple {
    my $self = shift;
    $logger->debug('read_tuple');

    my $tupmap = $self->read_command();

    return IO::Storm::Tuple->new(
        id        => $tupmap->{id},
        component => $tupmap->{comp},
        stream    => $tupmap->{stream},
        task      => $tupmap->{task},
        values    => $tupmap->{tuple}
    );
}

=method read_handshake

Read and process an initial handshake message from Storm

=cut

sub read_handshake {
    my $self = shift;

    # TODO: Figure out how to redirect stdout to ensure that print
    # statements/functions won't crash the Storm Java worker

    autoflush STDOUT 1;

    my $msg = $self->read_message();
    $logger->debug( 'Received initial handshake from Storm: %r', $msg );

    # Write a blank PID file out to the pidDir
    my $pid      = $$;
    my $pid_dir  = $msg->{pidDir};
    my $filename = $pid_dir . '/' . $pid;
    open my $fh, '>', $filename
        or die "Cant't write to '$filename': $!\n";
    $fh->close;
    $self->send_message( { pid => $pid } );
    $logger->debug('Process ID sent to Storm');

    return [ $msg->{conf}, $msg->{context} ];
}

=method send_message

Send a message to Storm, encoding it as JSON.

=cut

sub send_message {
    my ( $self, $msg ) = @_;
    say $self->_json->encode($msg);
    say "end";
}

=method sync

Send a sync command to Storm.

=cut

sub sync {
    my $self = shift;

    $self->send_message( { command => 'sync' } );
}

=method log

Send a log command to Storm

=cut

sub log {
    my $self    = shift;
    my $message = shift;

    $self->send_message( { command => 'log', msg => $message } );
}

1;

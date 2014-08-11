use v5.14;
use strict;
use warnings;

package IO::Storm;
# ABSTRACT: IO::Storm allows you to write Bolts and Spouts for Storm in Perl.

1;
__END__

=encoding utf8


=head1 SYNOPSIS

    package SplitSentenceBolt;

    use Moo;
    use namespace::clean;

    extends 'Storm::Bolt';

    sub process {
        my ($self, $tuple) = @_;

        my @words = split(' ', $tuple->values->[0]);
        foreach my $word (@words) {

            $self->emit([ $word ]);
        }

    }

    SplitSentenceBolt->new->run;


=head1 DESCRIPTION

IO::Storm allows you to leverage Storm's multilang support to write Bolts and
Spouts in Perl.  As of version 0.02, the API is designed to very closely mirror
that of the L<Streamparse Python library|http://streamparse.readthedocs.org/en/latest/api.html>.  The exception being that we don't currently support
the C<BatchingBolt> class or the C<emit_many> methods.


=head1 Bolts

To create a Bolt, you want to extend the C<Storm::Bolt> class.

    package SplitSentenceBolt;

    use Moo;
    use namespace::clean;

    extends 'Storm::Bolt';


=head2 Processing tuples

To have your Bolt start processing tuples, you want to override the C<process>
method, which takes a C<IO::Storm::Tuple> as its only argument.  This method
should do any processing you want to perform on the tuple and then C<emit> its
output.

    sub process {
        my ($self, $tuple) = @_;

        my @words = split(' ', $tuple->values->[0]);
        foreach my $word (@words) {

            $self->emit([ $word ]);
        }

    }

To actually start your Bolt, call the C<run> method, which will initialize the
bolt and start the event loop.

    SplitSentenceBolt->new->run;


=head2 Automatic reliability

By default, the Bolt will automatically handle acks, anchoring, and
failures.  If you would like to customize the behavior of any of these things,
you will need to set the C<auto_anchor>, C<auto_anchor>, or C<auto_fail>
attributes to 0.  For more information about Storm's guaranteed message
processing, please L<see their documentation|https://storm.incubator.apache.org/documentation/Guaranteeing-message-processing.html#what-is-storms-reliability-api>.


=head1 Spouts

To create a Spout, you want to extend the C<Storm::Spout> class.

    package SentenceSpout;

    use Moo;
    use namespace::clean;

    extends 'Storm::Spout';


=head2 Emitting tuples

To actually emit anything on your Spout, you have to implement the
C<next_tuple> method.

    my $sentences = ["a little brown dog",
                     "the man petted the dog",
                     "four score and seven years ago",
                     "an apple a day keeps the doctor away",];
    my $num_sentences = scalar(@$sentences);

    sub nextI<tuple {
        my ($self) = @>;

        $self->emit( [ $sentences->[ rand($num_sentences) ] ] );

    }

To actually start your Spout, call the C<run> method, which will initialize the
Spout and start the event loop.

    SentenceSpout->new->run;


=head1 Methods supported by Spouts and Bolts


=head2 Custom initialization

If you need to have some custom action happen when your component is being
initialized, just override C<initialize> method, which receives the Storm
configuration for the component and information about its place in the topology
as its arguments.

    sub initialize {
        my ( $self, $storm_conf, $context ) = @_;
    }


=head2 Logging

Use the C<log> method to send messages back to the Storm ShellBolt parent
process which will be added to the general Storm log.

    sub process {
        my ($self, $tuple) = @_;
        ...
        $self->log("Working on $tuple");
        ...
    }


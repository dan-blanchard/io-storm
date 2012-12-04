#!/usr/bin/env perl

#===============================================================================
#
#         FILE: Storm.t
#
#  DESCRIPTION: Test the IO::Storm class.
#
#===============================================================================

use strict;
use warnings;

use Data::Dumper;
use IO::Storm::Tuple;
use Test::MockObject;
use Test::More;
use Test::Output;
use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init($ERROR);

my $stdin = Test::MockObject->new();
my @stdin_retval = ();

$stdin->mock( 'getline', sub { return shift(@stdin_retval); } );

BEGIN { use_ok('IO::Storm'); }
my $storm = IO::Storm->new({_stdin=>$stdin});
my $result;

# Test read_message with simple data
push(@stdin_retval, '{"test":"test"}');
push(@stdin_retval, 'end');
$result = $storm->read_message;
is ( $result->{test}, "test", 'read_message() returns test output');

# read_task_ids
push(@stdin_retval, '{"test":"test0"}');
push(@stdin_retval, 'end');
push(@stdin_retval, '[2]');
push(@stdin_retval, 'end');
$result = $storm->read_task_ids;
is ( ref($result), 'ARRAY', 'read_task_ids() returns array');

# read_command
$storm = IO::Storm->new({_stdin=>$stdin});
push(@stdin_retval, '{"test":"test0"}');
push(@stdin_retval, 'end');
$result = $storm->read_command;
is ( ref($result), 'HASH', 'read_command() returns array');

# read_tuple
$storm = IO::Storm->new({_stdin=>$stdin});
push(@stdin_retval, '{"id":"test_id","stream":"test_stream","comp":"test_comp","tuple":["test"],"task":"test_task"}');
push(@stdin_retval, 'end');
push(@stdin_retval, '[2]');
push(@stdin_retval, 'end');
$result = $storm->read_task_ids;
is ( @{$storm->_pending_commands}[0]->{id}, 'test_id', 'read_tuple->id returns test_id');
my $tuple = $storm->read_tuple;
is ( ref($tuple), 'IO::Storm::Tuple', 'read_tuple returns tuple');
is ( $tuple->id, 'test_id', 'read_tuple->id returns test_id');

# send_msg_to_parent
sub test_send_message_to_parent { $storm->send_message_to_parent({test => "test"}); }
stdout_is(\&test_send_message_to_parent, '{"test":"test"}'."\nend\n", 'send_message_to_parent() returns test output');

# sync
sub test_sync { $storm->sync; }
stdout_is(\&test_sync, '{"command":"sync"}'."\nend\n", 'sync() returns right output');

# send_pid
sub test_send_pid { $storm->send_pid('./'); }
stdout_is(\&test_send_pid, '{"pid":"'.$$.'"}'."\nend\n", 'send_pid() returns right output');
# cleanup pid file
unlink($$);

# emit_bolt
sub test_emit_bolt { $storm->emit_bolt(["test"]); }
stdout_is(\&test_emit_bolt, '{"tuple":["test"],"command":"emit"}'."\nend\n", 'emit_bolt() returns right output');

# emit_spout
# emit
push(@stdin_retval, '[2]');
push(@stdin_retval, 'end');
sub test_emit { $storm->emit(["test"]); }
stdout_is(\&test_emit, '{"tuple":["test"],"command":"emit"}'."\nend\n", 'emit() returns right output');

# ack
sub test_ack { $storm->ack($tuple); }
stdout_is(\&test_ack, '{"id":"test_id","command":"ack"}'."\nend\n", 'ack() returns right output');

# fail
sub test_fail { $storm->fail($tuple); }
stdout_is(\&test_fail, '{"id":"test_id","command":"fail"}'."\nend\n", 'fail() returns right output');

# log
sub test_log { $storm->log('test_msg'); }
stdout_is(\&test_log, '{"msg":"test_msg","command":"log"}'."\nend\n", 'log() returns right output');

# init_component
push(@stdin_retval, '{"pidDir":"./","conf":"test_conf","context":"test_context"}');
push(@stdin_retval, 'end');
sub test_init_component { $result = $storm->init_component; }
stdout_is(\&test_init_component, '{"pid":"'.$$.'"}'."\nend\n", 'init_component() returns right output');
is ( @{$result}[0], 'test_conf', 'init_component returns correct conf');
is ( @{$result}[1], 'test_context', 'init_component returns correct context');
# cleanup pid file
unlink($$);

done_testing();

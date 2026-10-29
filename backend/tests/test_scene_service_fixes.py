"""Runtime contracts for the reproduced video workflow failures."""
import importlib
import json
from types import SimpleNamespace

import pytest
from langchain_core.messages import AIMessage, ToolMessage

from deerflow.agents.middlewares.computation_guard_middleware import ComputationGuardMiddleware

analytics = importlib.import_module('deerflow.tools.builtins.video_object_analytics_tool')


def output(skill, data=None, **extra):
    return ToolMessage(name='bash', tool_call_id=skill, content=json.dumps({'skill':skill, 'status':'success', 'data':data or {}, **extra}))


def plan():
    return output('city-video-intelligence', recommended_skill_chain=['camera-health-check', 'single-video-event-analysis'])


def test_plan_and_sampling_are_not_a_completed_event_review():
    assert '尚未执行' in ComputationGuardMiddleware._video_review_gaps([plan(), output('frame-sampling')])[0]


def test_failed_video_dependency_can_be_reported_without_looping():
    failed=ToolMessage(name='bash',tool_call_id='failed',content=json.dumps({'skill':'single-video-event-analysis','status':'failed','error_code':'VIDEO_NOT_FOUND'}))
    assert ComputationGuardMiddleware._video_review_gaps([plan(),failed]) == []


def test_event_review_must_successfully_view_the_actual_manifest_images():
    frame='/mnt/user-data/outputs/scene/frame_7000ms.jpg'
    prepared=output('single-video-event-analysis', {'frames':[{'image_uri':frame,'frame_id':'frame_7000ms'}]})
    call=AIMessage(content='',tool_calls=[{'name':'view_image','args':{'image_path':frame},'id':'view'}])
    failed=ToolMessage(name='view_image',tool_call_id='view',content='Error: Image file not found')
    assert ComputationGuardMiddleware._video_review_gaps([plan(),prepared,call,failed])
    viewed=ToolMessage(name='view_image',tool_call_id='view',content='Successfully read image')
    assert ComputationGuardMiddleware._video_review_gaps([plan(),prepared,call,viewed]) == []
    wrong=call.model_copy(update={'tool_calls':[{'name':'view_image','args':{'image_path':'/mnt/user-data/outputs/other.jpg'},'id':'view'}]})
    assert ComputationGuardMiddleware._video_review_gaps([plan(),prepared,wrong,viewed])


def test_empty_event_manifest_is_not_verified():
    assert ComputationGuardMiddleware._video_review_gaps([plan(),output('single-video-event-analysis',{'frames':[]})])


def test_object_only_workflow_does_not_require_event_analysis():
    assert ComputationGuardMiddleware._video_review_gaps([output('city-video-intelligence',recommended_skill_chain=['video-object-analytics'])]) == []


@pytest.mark.parametrize('native',[False,True])
def test_object_tool_exposes_sampling_mode_and_relative_times(monkeypatch,native):
    commands=[]
    frame={'frame_id':'f1','timestamp':None,'source_elapsed_seconds':.04,'timestamp_display':'00:00:00.040','time_reference':'relative','objects':[{'label':'person'}]}
    monkeypatch.setattr(analytics,'ensure_sandbox_initialized',lambda runtime:object())
    monkeypatch.setattr(analytics,'ensure_thread_directories_exist',lambda runtime:None)
    def run(runtime,sandbox,command):
        commands.append(command)
        return json.dumps({'detection':{'status':'success','data':{'detections':[frame]}}})
    monkeypatch.setattr(analytics,'_run_in_sandbox',run)
    result=json.loads(analytics.video_object_analytics_tool.func(runtime=SimpleNamespace(),video_path='/mnt/user-data/uploads/video.avi',capture_seconds=5,every_frame=native))
    assert result['status']=='success'
    assert result['sampling_mode']==('every_frame' if native else 'interval')
    assert ('--every-frame' in commands[0])==native
    assert '--started-at' not in commands[0] and '--camera-id' not in commands[0]
    assert result['camera_id'] is None
    assert result['summary']['per_frame'][0]['source_elapsed_seconds']==.04
    assert result['summary']['per_frame'][0]['time_reference']=='relative'
    assert result['summary']['unique_object_count'] is None


def test_video_review_cannot_be_skipped_after_a_plan_with_missing_source_was_executed():
    pending=output('city-video-intelligence',recommended_skill_chain=['single-video-event-analysis'],follow_up_question='视频来源？')
    assert ComputationGuardMiddleware._video_review_gaps([pending])==[]
    assert ComputationGuardMiddleware._video_review_gaps([pending,output('frame-sampling')])


@pytest.mark.parametrize('labels', ['person car truck bus','person,car,truck,bus','["person","car","truck","bus"]'])
def test_model_label_string_variants_are_normalized(labels):
    assert analytics._normalize_labels(labels)==['person','car','truck','bus']

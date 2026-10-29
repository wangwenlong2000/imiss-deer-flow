"""Export complete detection records; keep the sandbox stdout response bounded."""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path

PREVIEW_FRAMES = 20


def export(detections_path: Path, output_dir: Path, tracks_path: Path | None = None) -> dict:
    detection = json.loads(detections_path.read_text())
    if detection.get('status') != 'success':
        return {'detection': detection}
    output_dir.mkdir(parents=True, exist_ok=True)
    data = detection.get('data', {})
    per_frame, details, totals = [], [], Counter()
    for frame in data.get('detections', []):
        objects = frame.get('objects', [])
        counts = Counter(obj['label'] for obj in objects)
        totals.update(counts)
        common = {key: frame.get(key) for key in ['frame_id', 'timestamp', 'source_elapsed_seconds', 'timestamp_display', 'time_reference']}
        per_frame.append({**common, 'counts': dict(sorted(counts.items())), 'object_count': sum(counts.values())})
        for obj in objects:
            details.append({**common, 'object_id':obj.get('object_id'), 'label':obj.get('label'),
                            'confidence':obj.get('confidence'), 'bbox':obj.get('bbox')})
    frame_csv = output_dir / 'per_frame_counts.csv'
    detail_csv = output_dir / 'detection_details.csv'
    fields = ['frame_id','timestamp','source_elapsed_seconds','timestamp_display','time_reference']
    for path, rows, columns in [(frame_csv,per_frame,fields+['counts','object_count']),
                                 (detail_csv,details,fields+['object_id','label','confidence','bbox'])]:
        with path.open('w', newline='', encoding='utf-8') as handle:
            writer=csv.DictWriter(handle,fieldnames=columns)
            writer.writeheader()
            writer.writerows({k:json.dumps(v,ensure_ascii=False,separators=(',',':')) if isinstance(v,(dict,list)) else v for k,v in row.items()} for row in rows)
    summary={'frame_count':len(per_frame), 'per_frame':per_frame[:PREVIEW_FRAMES],
             'per_frame_preview_only':len(per_frame)>PREVIEW_FRAMES,
             'frame_detection_totals':dict(sorted(totals.items())), 'total_frame_detections':sum(totals.values()),
             'unique_object_count':None, 'unique_object_count_note':'Frame detections are not deduplicated identities.',
             'model':data.get('model')}
    result={'detection':{'status':'success','data':{'summary':summary}},
            'artifacts':{'per_frame_csv':str(frame_csv),'detection_details_csv':str(detail_csv)}}
    if tracks_path:
        tracking=json.loads(tracks_path.read_text())
        tracks=tracking.get('data',{}).get('tracks',[])
        result['tracking']={'status':tracking.get('status'),'data':{'summary':{
            'track_count':len(tracks),
            'by_label':dict(Counter(t.get('label') for t in tracks)),
            'by_movement_state':dict(Counter(t.get('movement_state') for t in tracks))}}}
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--detections',type=Path,required=True)
    parser.add_argument('--tracks',type=Path)
    parser.add_argument('--output-dir',type=Path,required=True)
    args=parser.parse_args()
    print(json.dumps(export(args.detections,args.output_dir,args.tracks),ensure_ascii=False,separators=(',',':')))

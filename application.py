""" flask server application """

import pickle

from flask import Flask, render_template, request, abort
from flask_socketio import SocketIO, Namespace, emit, join_room

from paginate import Pagination, get_page_args
import db

application = Flask(__name__)
application.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(application)


def get_data(subject_id=None, database=None):
    if database is None:
        database = db.Database()
    if subject_id is None:
        print("no subject id specified")
        subject_id = database.popular_subjects(limit=1)[0][1]

    posts = database.get_story(subject_id)

    ret_data = {
        'subject': subject_id,
        'used': [
            {
                'id': i,
                'text': str(pickle.loads(s)),
                'link': l
            }
            for i, s, l, u in posts
            if u],
        'available': [
            {
                'id': i,
                'text': str(pickle.loads(s)),
                'link': l
            }
            for i, s, l, u in posts
            if not u]
    }
    return ret_data


class Topics(Namespace):
    def on_join(self, message):
        join_room(message['room'])
        emit('update', {'data': get_data(subject_id=message['subject_id'])})

    def on_submit(self, message):
        database = db.Database()
        database.append_to_story(message['subject_id'], message['sentence_id'])
        emit(
            'update',
            {'data': get_data(subject_id=message['subject_id'], database=database)},
            room=message['room'])


socketio.on_namespace(Topics(''))

@application.route("/favicon.ico")
def favicon():
    abort(404)


@application.route("/")
def topics():
    page, per_page, offset = get_page_args(
        page_parameter='page',
        per_page_parameter='per_page')
    order = request.args.get('order') or "time"

    database = db.Database()
    subjects = database.get_topics(order, offset=offset, per_page=per_page)
    pagination = Pagination(
        css_framework='bootstrap4',
        link_size='sm',
        show_single_page=False,
        page=page,
        per_page=per_page,
        total=database.get_num_topics(),
        record_name='Topics',
        format_total=True,
        format_number=True,
    )

    context = {
        'subjects': subjects,
        'pagination': pagination
    }
    return render_template('topics.html', **context)


@application.route("/<subject_id>")
def subject(subject_id):
    database = db.Database()
    sub = database.get_subject_by_id(subject_id)
    context = {
        'subject': {
            'id': sub[0],
            'text': sub[1].upper(),
        }
    }
    return render_template('subject.html', **context)


if __name__ == '__main__':
    socketio.run(application)

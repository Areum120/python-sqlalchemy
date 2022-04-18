from datetime import datetime
from db import db


# create table

# User라는 오브젝트
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(20), unique=True, nullable=False)
    email = db.Column(db.String(20), unique=True, nullable=False)
    image_file = db.Column(db.String(20), nullable=False, default='default.jpg')
    password = db.Column(db.String(60), nullable=False, default='default.jpg')
    posts = db.relationship('Post', backref='author', lazy=True)

    def __repr__(self):
        return f"User('{self.username}', '{self.email}', '{self.image_file}')"

# Post라는 오브젝트
class Post(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100), nullable=False)
    date_posted = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    content = db.Column(db.Text, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)

    def __repr__(self):
        return f"Post('{self.id}', '{self.title}', '{self.date_posted}')"

# subsidy_info라는 오브젝트
class subsidy_info(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sido = db.Column(db.String(20), nullable=False)
    region = db.Column(db.String(20), nullable=False)
    num_notice_all = db.Column(db.Integer, nullable=False)
    num_notice_priority = db.Column(db.Integer, nullable=False)
    num_notice_corp = db.Column(db.Integer, nullable=False)
    num_notice_taxi = db.Column(db.Integer, nullable=False)
    num_notice_normal = db.Column(db.Integer, nullable=False)
    num_recept_all = db.Column(db.Integer, nullable=False)
    num_recept_priority = db.Column(db.Integer, nullable=False)
    num_recept_corp = db.Column(db.Integer, nullable=False)
    num_recept_taxi = db.Column(db.Integer, nullable=False)
    num_recept_normal = db.Column(db.Integer, nullable=False)
    num_release_all = db.Column(db.Integer, nullable=False)
    num_release_priority = db.Column(db.Integer, nullable=False)
    num_release_corp = db.Column(db.Integer, nullable=False)
    num_release_taxi = db.Column(db.Integer, nullable=False)
    num_release_normal = db.Column(db.Integer, nullable=False)
    num_remains_all = db.Column(db.Integer, nullable=False)
    num_remains_priority = db.Column(db.Integer, nullable=False)
    num_remains_corp = db.Column(db.Integer, nullable=False)
    num_remains_taxi = db.Column(db.Integer, nullable=False)
    num_remains_normal = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return f"subsidy_info('{self.id}', '{self.sido}', '{self.region}', '{self.num_notice_all}', '{self.num_notice_priority}', '{self.num_notice_corp}', '{self.num_notice_taxi}', '{self.num_notice_normal}', '{self.num_recept_all}', '{self.num_recept_priority}', '{self.num_recept_corp}', '{self.num_recept_taxi}', '{self.num_recept_normal}', '{self.num_release_all}', '{self.num_release_priority}', '{self.num_release_corp}', '{self.num_release_taxi}', '{self.num_release_normal}', '{self.num_remains_all}', '{self.num_remains_priority}', '{self.num_remains_corp}', '{self.num_remains_taxi}', '{self.num_remains_normal}', '{self.created_at}', '{self.updated_at}')"


# subsidy_accepted라는 오브젝트
class subsidy_accepted(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    num_remains_all = db.Column(db.Integer, primary_key=True)
    num_remains_priority = db.Column(db.Integer, nullable=False)
    num_remains_corp = db.Column(db.Integer, nullable=False)
    num_remains_taxi = db.Column(db.Integer, nullable=False)
    num_remains_normal = db.Column(db.Integer, nullable=False)
    acceptance_rate_all =db.Column(db.Float, nullable=False)
    acceptance_rate_priority = db.Column(db.Float, nullable=False)
    acceptance_rate_corp = db.Column(db.Float, nullable=False)
    acceptance_rate_taxi = db.Column(db.Float, nullable=False)
    acceptance_rate_normal =db.Column(db.Float, nullable=False)
    availability = db.Column(db.BOOLEAN, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return f"Polestar_info('{self.id}', '{self.num_remains_all}', '{self.num_remains_priority}', '{self.num_remains_corp}', '{self.num_remains_taxi}', '{self.num_remains_normal}', '{self.acceptance_rate_all}', '{self.acceptance_rate_priority}', '{self.acceptance_rate_corp}', '{self.acceptance_rate_taxi}', '{self.acceptance_rate_normal}', '{self.availability}', '{self.created_at}', '{self.updated_at}')"



# subsidy_trend라는 오브젝트
class subsidy_trend(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DATE, nullable=False, default=datetime.utcnow)
    num_notice_all = db.Column(db.Integer, nullable=False)
    num_notice_priority = db.Column(db.Integer, nullable=False)
    num_notice_corp = db.Column(db.Integer, nullable=False)
    num_notice_taxi = db.Column(db.Integer, nullable=False)
    num_notice_normal = db.Column(db.Integer, nullable=False)
    num_daily_recept_all = db.Column(db.Integer, nullable=False)
    num_daily_recept_priority = db.Column(db.Integer, nullable=False)
    num_daily_recept_corp = db.Column(db.Integer, nullable=False)
    num_daily_recept_taxi = db.Column(db.Integer, nullable=False)
    num_daily_recept_normal = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return f"Polestar_info('{self.id}', '{self.date}', '{self.num_notice_all}', '{self.num_notice_priority}', '{self.num_notice_corp}', '{self.num_notice_taxi}', '{self.num_notice_normal}', '{self.num_daily_recept_all}', '{self.num_daily_recept_priority}', '{self.num_daily_recept_corp}', '{self.num_daily_recept_taxi}', '{self.num_daily_recept_normal}', '{self.created_at}', '{self.updated_at}')"


# subsidy_closing_area라는 오브젝트

class subsidy_closing_area(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sido = db.Column(db.String(20), nullable=False)
    region = db.Column(db.String(20), nullable=False)
    acceptance_rate_all = db.Column(db.Float, nullable=False)
    deadline = db.Column(db.BOOLEAN, nullable=False)
    due_to_close = db.Column(db.BOOLEAN, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return f"Polestar_info('{self.id}', '{self.sido}', '{self.region}', '{self.acceptance_rate_all}', '{self.deadline}', '{self.due_to_close}', '{self.created_at}', '{self.updated_at}')"


# check_subsidy_area라는 오브젝트

class check_subsidy_area(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sido = db.Column(db.String(20), nullable=False)
    region = db.Column(db.String(20), nullable=False)
    subsidy_riding = db.Column(db.Integer, nullable=False)
    subsidy_compact = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return f"Polestar_info('{self.id}', '{self.sido}', '{self.region}', '{self.subsidy_riding}', '{self.subsidy_compact}', '{self.created_at}', '{self.updated_at}')"

    print('table 생성 완료')
    

# check_subsidy_car라는 오브젝트

class check_subsidy_car(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    category = db.Column(db.String(10), nullable=False)
    importer = db.Column(db.String(30), nullable=False)
    model = db.Column(db.String(100), nullable=False)
    subsidy = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return f"Polestar_info('{self.id}', '{self.category}', '{self.importer}', '{self.model}', '{self.subsidy}', '{self.created_at}', '{self.updated_at}')"
        print('table 생성 완료')
    

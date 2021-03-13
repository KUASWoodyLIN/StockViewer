from datetime import datetime
from flask import Flask, render_template
from flask_bootstrap import Bootstrap
from flask_moment import Moment
from units.draw import *
from units.sql import StockSQL
from flask_wtf import FlaskForm
from wtforms import SubmitField, DateField, SelectMultipleField, IntegerField, StringField
from wtforms.validators import DataRequired

app = Flask(__name__)
app.config['SECRET_KEY'] = 'woodylin7158517'

bootstrap = Bootstrap(app)
moment = Moment(app)
stock_sql = StockSQL()


class StocksForm(FlaskForm):
    start_date = DateField('開始時間', id='datepick', validators=[DataRequired()])
    end_date = DateField('結束時間', validators=[DataRequired()])
    stock_id = IntegerField('股票代號', validators=[DataRequired()])
    submit = SubmitField('送出')


@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_server_error(e):
    return render_template('500.html'), 500


@app.route('/')
def index():
    return render_template('index.html',
                           current_time=datetime.utcnow())


@app.route('/test/', methods=['GET', 'POST'])
def classes2area_page():
    html_str = ''
    form = StocksForm()
    start_date = ''
    end_date = ''
    if form.validate_on_submit():
        stock_id = form.stock_id.data
        start_date = form.start_date.data
        end_date = form.end_date.data
        form.stock_id.data = ''
        form.start_date.data = ''
        form.end_date.data = ''
        chart_data = stock_sql.read_stock_values_and_pred(stock_id)
        html_str = draw_kline(chart_data)

    return render_template(
        'test.html',
        form=form,
        content=html_str,
    )


@app.route('/user/<name>')
def user(name):
    return render_template('user.html', name=name)


if __name__ == '__main__':
    app.run()
